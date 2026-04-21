package vexobj

import (
	"net/url"
	"testing"
)

func TestImageURL(t *testing.T) {
	c := New("http://localhost:8000", "vex_test")

	u := c.ImageURL("photos", "cat.jpg", &ImageTransform{
		Width:   300,
		Height:  200,
		Format:  "webp",
		Quality: 80,
		Fit:     "cover",
	})

	parsed, err := url.Parse(u)
	if err != nil {
		t.Fatal(err)
	}

	q := parsed.Query()
	if q.Get("w") != "300" {
		t.Errorf("expected w=300, got %s", q.Get("w"))
	}
	if q.Get("h") != "200" {
		t.Errorf("expected h=200, got %s", q.Get("h"))
	}
	if q.Get("format") != "webp" {
		t.Errorf("expected format=webp, got %s", q.Get("format"))
	}
	if q.Get("quality") != "80" {
		t.Errorf("expected quality=80, got %s", q.Get("quality"))
	}
	if q.Get("fit") != "cover" {
		t.Errorf("expected fit=cover, got %s", q.Get("fit"))
	}
}

func TestImageURLNoTransform(t *testing.T) {
	c := New("http://localhost:8000", "vex_test")
	u := c.ImageURL("photos", "cat.jpg", nil)

	if u != "http://localhost:8000/v1/objects/photos/cat.jpg" {
		t.Errorf("unexpected URL: %s", u)
	}
}

func TestNewClient(t *testing.T) {
	c := New("http://localhost:8000/", "vex_mykey")
	if c.BaseURL != "http://localhost:8000" {
		t.Errorf("trailing slash not trimmed: %s", c.BaseURL)
	}
	if c.APIKey != "vex_mykey" {
		t.Errorf("wrong api key: %s", c.APIKey)
	}
}
