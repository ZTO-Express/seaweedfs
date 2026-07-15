package command

import "testing"

func TestStaticWebsiteFlagsDisabledByDefault(t *testing.T) {
	tests := []struct {
		name       string
		value      *bool
		registered bool
	}{
		{
			name:       "standalone s3",
			value:      s3StandaloneOptions.enableStaticWebsite,
			registered: cmdS3.Flag.Lookup("enableStaticWebsite") != nil,
		},
		{
			name:       "embedded server",
			value:      s3Options.enableStaticWebsite,
			registered: cmdServer.Flag.Lookup("s3.enableStaticWebsite") != nil,
		},
		{
			name:       "embedded filer",
			value:      filerS3Options.enableStaticWebsite,
			registered: cmdFiler.Flag.Lookup("s3.enableStaticWebsite") != nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if !test.registered || test.value == nil {
				t.Fatal("static website flag is not registered")
			}
			if *test.value {
				t.Fatal("static website flag must be disabled by default")
			}
		})
	}
}
