package config

import "testing"

func TestCESConfigForExtensionUsesConfiguredApp(t *testing.T) {
	cfg := CESConfig{
		ProjectID:     "project",
		Location:      "us",
		Endpoint:      "ces.googleapis.com:443",
		Transport:     TransportGRPC,
		SessionPrefix: "sip",
		Extensions: map[string]CESExtensionConfig{
			"1111": {
				AppID:        "app-1111",
				DeploymentID: "deployment-1111",
			},
		},
	}

	got, ok := cfg.ForExtension("1111")
	if !ok {
		t.Fatal("extension 1111 was not accepted")
	}
	if got.ProjectID != "project" {
		t.Fatalf("project_id = %q, want project", got.ProjectID)
	}
	if got.Location != "us" {
		t.Fatalf("location = %q, want us", got.Location)
	}
	if got.AppID != "app-1111" {
		t.Fatalf("app_id = %q, want app-1111", got.AppID)
	}
	if got.DeploymentID != "deployment-1111" {
		t.Fatalf("deployment_id = %q, want deployment-1111", got.DeploymentID)
	}
	if len(got.Extensions) != 0 {
		t.Fatalf("resolved config kept extension map: %#v", got.Extensions)
	}
}

func TestCESConfigForExtensionRejectsUnknownExtension(t *testing.T) {
	cfg := CESConfig{
		ProjectID: "project",
		Location:  "us",
		Extensions: map[string]CESExtensionConfig{
			"1111": {AppID: "app-1111"},
		},
	}

	if _, ok := cfg.ForExtension("2222"); ok {
		t.Fatal("unknown extension was accepted")
	}
}

func TestCESConfigValidateAllowsExtensionMappedApps(t *testing.T) {
	cfg := CESConfig{
		ProjectID: "project",
		Location:  "us",
		Extensions: map[string]CESExtensionConfig{
			"1111": {AppID: "app-1111"},
		},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestCESConfigValidateKeepsSingleAppConfigValid(t *testing.T) {
	cfg := CESConfig{
		ProjectID: "project",
		Location:  "us",
		AppID:     "app",
	}

	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}
