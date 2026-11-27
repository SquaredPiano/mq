package main

import (
	"crypto/tls"
	"errors"
	"log"
	"os"
	"sync"
	"time"
)

type CertificateLoader struct {
	mu        sync.RWMutex
	cert      *tls.Certificate
	certFile  string
	keyFile   string
	certMtime time.Time
	keyMtime  time.Time
}

func NewCertificateLoader(certFile, keyFile string) (*CertificateLoader, error) {
	l := &CertificateLoader{certFile: certFile, keyFile: keyFile}
	if err := l.reloadIfChanged(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *CertificateLoader) getCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	if err := l.reloadIfChanged(); err != nil {
		return nil, err
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.cert == nil {
		return nil, errors.New("no TLS certificate loaded")
	}
	return l.cert, nil
}

func (l *CertificateLoader) reloadIfChanged() error {
	certInfo, err := os.Stat(l.certFile)
	if err != nil {
		return err
	}
	keyInfo, err := os.Stat(l.keyFile)
	if err != nil {
		return err
	}

	l.mu.RLock()
	same := l.cert != nil && l.certMtime == certInfo.ModTime() && l.keyMtime == keyInfo.ModTime()
	l.mu.RUnlock()
	if same {
		return nil
	}

	cert, err := tls.LoadX509KeyPair(l.certFile, l.keyFile)
	if err != nil {
		return err
	}
	if len(cert.Certificate) == 0 {
		return errors.New("certificate file contains no certificates")
	}

	l.mu.Lock()
	l.cert = &cert
	l.certMtime = certInfo.ModTime()
	l.keyMtime = keyInfo.ModTime()
	l.mu.Unlock()
	log.Printf("loaded TLS certificate from %s", l.certFile)
	return nil
}

func (l *CertificateLoader) tlsConfig() *tls.Config {
	return &tls.Config{
		MinVersion:     tls.VersionTLS12,
		GetCertificate: l.getCertificate,
	}
}

func (l *CertificateLoader) watch(stop <-chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			if err := l.reloadIfChanged(); err != nil {
				log.Printf("tls certificate reload check failed: %v", err)
			}
		}
	}
}
