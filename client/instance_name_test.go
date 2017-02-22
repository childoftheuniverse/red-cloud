package client

import "testing"

func TestSplitTablePath(t *testing.T) {
	var instance, table string
	var err error

	instance, table, err = SplitTablePath("red-cloud://inst/tab")
	if err != nil {
		t.Fatalf("SplitTablePath failed with error %s", err.Error())
	}
	if instance != "inst" {
		t.Errorf("Unexpected instance \"%s\" (expected inst)", instance)
	}
	if table != "tab" {
		t.Errorf("Unexpected table \"%s\" (expected tab)", table)
	}

	instance, table, err = SplitTablePath("red-cloud://inst/tab/le")
	if err != nil {
		t.Fatalf("SplitTablePath failed with error %s", err.Error())
	}
	if instance != "inst" {
		t.Errorf("Unexpected instance \"%s\" (expected inst)", instance)
	}
	if table != "tab" {
		t.Errorf("Unexpected table \"%s\" (expected tab)", table)
	}

	_, _, err = SplitTablePath("http://www/path")
	if err == nil || err.Error() != "Unsupported schema: http" {
		t.Fatalf("SplitTablePath failed with error %s", err.Error())
	}

	instance, table, err = SplitTablePath("red-cloud://inst")
	if err != nil {
		t.Fatalf("SplitTablePath failed with error %s", err.Error())
	}
	if instance != "inst" {
		t.Errorf("Unexpected instance \"%s\" (expected inst)", instance)
	}
	if table != "" {
		t.Errorf("Unexpected table \"%s\" (expected it to be empty)", table)
	}
}
