// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package io

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/auth/bearer"
	"github.com/wolfeidau/s3iofs"
)

// Constants for S3 configuration options
const (
	S3Region          = "s3.region"
	S3SessionToken    = "s3.session-token"
	S3SecretAccessKey = "s3.secret-access-key"
	S3AccessKeyID     = "s3.access-key-id"
	S3EndpointURL     = "s3.endpoint"
	S3ProxyURI        = "s3.proxy-uri"
)

func createS3FileIO(parsed *url.URL, props map[string]string) (IO, error) {
	cfgOpts := []func(*config.LoadOptions) error{}
	opts := []func(*s3.Options){}

	endpoint, ok := props[S3EndpointURL]
	if !ok {
		endpoint = os.Getenv("AWS_S3_ENDPOINT")
	}

	if endpoint != "" {
		opts = append(opts, func(o *s3.Options) {
			o.BaseEndpoint = &endpoint
		})
	}

	if tok, ok := props["token"]; ok {
		cfgOpts = append(cfgOpts, config.WithBearerAuthTokenProvider(
			&bearer.StaticTokenProvider{Token: bearer.Token{Value: tok}}))
	}

	if region, ok := props[S3Region]; ok {
		opts = append(opts, func(o *s3.Options) {
			o.Region = region
		})
	} else if region, ok := props["client.region"]; ok {
		opts = append(opts, func(o *s3.Options) {
			o.Region = region
		})
	}

	accessKey, secretAccessKey := props[S3AccessKeyID], props[S3SecretAccessKey]
	token := props[S3SessionToken]
	if accessKey != "" || secretAccessKey != "" || token != "" {
		opts = append(opts, func(o *s3.Options) {
			o.Credentials = credentials.NewStaticCredentialsProvider(
				props[S3AccessKeyID], props[S3SecretAccessKey], props[S3SessionToken])
		})
	}

	if proxy, ok := props[S3ProxyURI]; ok {
		proxyURL, err := url.Parse(proxy)
		if err != nil {
			return nil, fmt.Errorf("invalid s3 proxy url '%s'", proxy)
		}

		opts = append(opts, func(o *s3.Options) {
			o.HTTPClient = awshttp.NewBuildableClient().WithTransportOptions(
				func(t *http.Transport) { t.Proxy = http.ProxyURL(proxyURL) })
		})
	}

	awscfg, err := config.LoadDefaultConfig(context.Background(), cfgOpts...)
	if err != nil {
		return nil, err
	}

	s3Client := s3.NewFromConfig(awscfg, opts...)
	preprocess := func(n string) string {
		_, after, found := strings.Cut(n, "://")
		if found {
			n = after
		}

		return strings.TrimPrefix(n, parsed.Host)
	}

	s3fs := s3iofs.NewWithClient(parsed.Host, s3Client)
	return FSPreProcName(s3fs, preprocess), nil
}
