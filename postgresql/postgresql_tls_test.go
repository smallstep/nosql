//go:build !nopgx

package postgresql

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// writeClientCert writes a self-signed client certificate and key to the given
// paths, overwriting whatever is there, and returns its common name.
func writeClientCert(t *testing.T, certPath, keyPath, commonName string) string {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("creating certificate: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatalf("writing certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshalling key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("writing key: %v", err)
	}
	return commonName
}

// commonNameOf reads the subject CN out of whatever the handshake would offer.
func commonNameOf(t *testing.T, config *pgx.ConnConfig) string {
	t.Helper()

	if config.TLSConfig == nil || config.TLSConfig.GetClientCertificate == nil {
		t.Fatal("no GetClientCertificate callback installed")
	}
	cert, err := config.TLSConfig.GetClientCertificate(nil)
	if err != nil {
		t.Fatalf("GetClientCertificate: %v", err)
	}
	if len(cert.Certificate) == 0 {
		return ""
	}
	parsed, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("parsing offered certificate: %v", err)
	}
	return parsed.Subject.CommonName
}

func tlsDataSource(t *testing.T) (dsn, certPath, keyPath string) {
	t.Helper()
	dir := t.TempDir()
	certPath = filepath.Join(dir, "tls.crt")
	keyPath = filepath.Join(dir, "tls.key")
	dsn = "postgresql://user@localhost:5432/db?sslmode=require" +
		"&sslcert=" + certPath + "&sslkey=" + keyPath
	return dsn, certPath, keyPath
}

// A rotated certificate must be picked up without restarting the process.
// ParseConfig reads the files once, so without the reload every later
// connection keeps offering the certificate that was on disk at startup.
func TestReloadClientCertificatesPicksUpRotation(t *testing.T) {
	dsn, certPath, keyPath := tlsDataSource(t)
	writeClientCert(t, certPath, keyPath, "cert-A")

	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parsing DSN: %v", err)
	}
	reloadClientCertificates(config, dsn)

	if got := commonNameOf(t, config); got != "cert-A" {
		t.Fatalf("before rotation: got %q, want %q", got, "cert-A")
	}

	// Rotate underneath the running process, as a CSI volume would.
	writeClientCert(t, certPath, keyPath, "cert-B")

	if got := commonNameOf(t, config); got != "cert-B" {
		t.Fatalf("after rotation: got %q, want %q", got, "cert-B")
	}
}

// The stale copy must not remain reachable, or a future change that prefers
// Certificates over the callback would silently reintroduce the bug.
func TestReloadClientCertificatesClearsTheLoadedCopy(t *testing.T) {
	dsn, certPath, keyPath := tlsDataSource(t)
	writeClientCert(t, certPath, keyPath, "cert-A")

	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parsing DSN: %v", err)
	}
	if len(config.TLSConfig.Certificates) != 1 {
		t.Fatalf("expected ParseConfig to load one certificate, got %d",
			len(config.TLSConfig.Certificates))
	}

	reloadClientCertificates(config, dsn)

	if len(config.TLSConfig.Certificates) != 0 {
		t.Fatalf("expected the eagerly loaded certificate to be cleared, got %d",
			len(config.TLSConfig.Certificates))
	}
}

// A DSN with no client certificate must be left exactly as it was.
func TestReloadClientCertificatesIgnoresDSNWithoutClientCert(t *testing.T) {
	for _, dsn := range []string{
		"postgresql://user@localhost:5432/db?sslmode=require",
		"postgresql://user@localhost:5432/db?sslmode=disable",
	} {
		config, err := pgx.ParseConfig(dsn)
		if err != nil {
			t.Fatalf("parsing %q: %v", dsn, err)
		}

		reloadClientCertificates(config, dsn)

		if config.TLSConfig != nil && config.TLSConfig.GetClientCertificate != nil {
			t.Fatalf("%q: installed a callback where there is no client certificate", dsn)
		}
	}
}
