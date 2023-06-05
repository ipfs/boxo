package gateway

import (
	"fmt"
	"io"
	"net/http"
	gopath "path"
	"strconv"
	"strings"

	"go.uber.org/zap"

	ipath "github.com/ipfs/boxo/coreiface/path"
	"github.com/ipfs/boxo/files"

	redirects "github.com/ipfs/go-ipfs-redirects-file"
)

// Resolving a UnixFS path involves determining if the provided `path.Path` exists and returning the `path.Resolved`
// corresponding to that path. For UnixFS, path resolution is more involved.
//
// When a path under requested CID does not exist, Gateway will check if a `_redirects` file exists
// underneath the root CID of the path, and apply rules defined there.
// See sepcification introduced in: https://github.com/ipfs/specs/pull/290
//
// Scenario 1:
// If a path exists, we always return the `path.Resolved` corresponding to that path, regardless of the existence of a `_redirects` file.
//
// Scenario 2:
// If a path does not exist, usually we should return a `nil` resolution path and an error indicating that the path
// doesn't exist.  However, a `_redirects` file may exist and contain a redirect rule that redirects that path to a different path.
// We need to evaluate the rule and perform the redirect if present.
//
// Scenario 3:
// Another possibility is that the path corresponds to a rewrite rule (i.e. a rule with a status of 200).
// In this case, we don't perform a redirect, but do need to return a `path.Resolved` and `path.Path` corresponding to
// the rewrite destination path.
//
// Note that for security reasons, redirect rules are only processed when the request has origin isolation.
// See https://github.com/ipfs/specs/pull/290 for more information.
func (i *handler) serveRedirectsIfPresent(w http.ResponseWriter, r *http.Request, maybeResolvedImPath, immutableContentPath ImmutablePath, contentPath ipath.Path, logger *zap.SugaredLogger) (newContentPath ImmutablePath, continueProcessing bool, hadMatchingRule bool) {
	// contentPath is the full ipfs path to the requested resource,
	// regardless of whether path or subdomain resolution is used.
	rootPath := getRootPath(immutableContentPath)
	redirectsPath := ipath.Join(rootPath, "_redirects")
	imRedirectsPath, err := NewImmutablePath(redirectsPath)
	if err != nil {
		err = fmt.Errorf("trouble processing _redirects path %q: %w", redirectsPath, err)
		i.webError(w, r, err, http.StatusInternalServerError)
		return ImmutablePath{}, false, true
	}

	foundRedirect, redirectRules, err := i.getRedirectRules(r, imRedirectsPath)
	if err != nil {
		err = fmt.Errorf("trouble processing _redirects file at %q: %w", redirectsPath, err)
		i.webError(w, r, err, http.StatusInternalServerError)
		return ImmutablePath{}, false, true
	}

	if foundRedirect {
		redirected, newPath, err := i.handleRedirectsFileRules(w, r, immutableContentPath, contentPath, redirectRules, logger)
		if err != nil {
			err = fmt.Errorf("trouble processing _redirects file at %q: %w", redirectsPath, err)
			i.webError(w, r, err, http.StatusInternalServerError)
			return ImmutablePath{}, false, true
		}

		if redirected {
			return ImmutablePath{}, false, true
		}

		// 200 is treated as a rewrite, so update the path and continue
		if newPath != "" {
			// Reassign contentPath and resolvedPath since the URL was rewritten
			p := ipath.New(newPath)
			imPath, err := NewImmutablePath(p)
			if err != nil {
				err = fmt.Errorf("could not use _redirects file to %q: %w", p, err)
				i.webError(w, r, err, http.StatusInternalServerError)
				return ImmutablePath{}, false, true
			}
			return imPath, true, true
		}
	}

	// No matching rule, paths remain the same, continue regular processing
	return maybeResolvedImPath, true, false
}

func (i *handler) handleRedirectsFileRules(w http.ResponseWriter, r *http.Request, immutableContentPath ImmutablePath, cPath ipath.Path, redirectRules []redirects.Rule, logger *zap.SugaredLogger) (redirected bool, newContentPath string, err error) {
	// Attempt to match a rule to the URL path, and perform the corresponding redirect or rewrite
	pathParts := strings.Split(immutableContentPath.String(), "/")
	if len(pathParts) > 3 {
		// All paths should start with /ipfs/cid/, so get the path after that
		urlPath := "/" + strings.Join(pathParts[3:], "/")
		rootPath := strings.Join(pathParts[:3], "/")
		// Trim off the trailing /
		urlPath = strings.TrimSuffix(urlPath, "/")

		for _, rule := range redirectRules {
			// Error right away if the rule is invalid
			if !rule.MatchAndExpandPlaceholders(urlPath) {
				continue
			}

			// We have a match!

			// Rewrite
			if rule.Status == 200 {
				// Prepend the rootPath
				toPath := rootPath + rule.To
				return false, toPath, nil
			}

			// Or 4xx
			if rule.Status == 404 || rule.Status == 410 || rule.Status == 451 {
				toPath := rootPath + rule.To
				imContent4xxPath, err := NewImmutablePath(ipath.New(toPath))
				if err != nil {
					return true, toPath, err
				}

				// While we have the immutable path which is enough to fetch the data we need to track mutability for
				// headers.
				contentPathParts := strings.Split(cPath.String(), "/")
				if len(contentPathParts) <= 3 {
					// Match behavior as with the immutable path
					return false, "", nil
				}
				// All paths should start with /ip(f|n)s/<root>/, so get the path after that
				contentRootPath := strings.Join(contentPathParts[:3], "/")
				content4xxPath := ipath.New(contentRootPath + rule.To)
				err = i.serve4xx(w, r, imContent4xxPath, content4xxPath, rule.Status, logger)
				return true, toPath, err
			}

			// Or redirect
			if rule.Status >= 301 && rule.Status <= 308 {
				http.Redirect(w, r, rule.To, rule.Status)
				return true, "", nil
			}
		}
	}

	// No redirects matched
	return false, "", nil
}

// getRedirectRules fetches the _redirects file corresponding to a given path and returns the rules
// Returns whether _redirects was found, the rules (if they exist) and if there was an error (other than a missing _redirects)
// If there is an error returns (false, nil, err)
func (i *handler) getRedirectRules(r *http.Request, redirectsPath ImmutablePath) (bool, []redirects.Rule, error) {
	// Check for _redirects file.
	// Any path resolution failures are ignored and we just assume there's no _redirects file.
	// Note that ignoring these errors also ensures that the use of the empty CID (bafkqaaa) in tests doesn't fail.
	_, redirectsFileGetResp, err := i.backend.Get(r.Context(), redirectsPath)
	if err != nil {
		if isErrNotFound(err) {
			return false, nil, nil
		}
		return false, nil, err
	}

	if redirectsFileGetResp.bytes == nil {
		return false, nil, fmt.Errorf(" _redirects is not a file")
	}
	f := redirectsFileGetResp.bytes
	defer f.Close()

	// Parse redirect rules from file
	redirectRules, err := redirects.Parse(f)
	if err != nil {
		return false, nil, fmt.Errorf("could not parse _redirects: %w", err)
	}
	return true, redirectRules, nil
}

// Returns the root CID Path for the given path
func getRootPath(path ipath.Path) ipath.Path {
	parts := strings.Split(path.String(), "/")
	return ipath.New(gopath.Join("/", path.Namespace(), parts[2]))
}

func (i *handler) serve4xx(w http.ResponseWriter, r *http.Request, content4xxPathImPath ImmutablePath, content4xxPath ipath.Path, status int, logger *zap.SugaredLogger) error {
	pathMetadata, getresp, err := i.backend.Get(r.Context(), content4xxPathImPath)
	if err != nil {
		return err
	}

	if getresp.bytes == nil {
		return fmt.Errorf("could not convert node for %d page to file", status)
	}
	content4xxFile := getresp.bytes
	defer content4xxFile.Close()

	content4xxCid := pathMetadata.LastSegment.Cid()

	size, err := content4xxFile.Size()
	if err != nil {
		return fmt.Errorf("could not get size of %d page", status)
	}

	logger.Debugf("using _redirects: custom %d file at %q", status, content4xxPath)
	w.Header().Set("Content-Type", "text/html")
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	addCacheControlHeaders(w, r, content4xxPath, content4xxCid, "")
	w.WriteHeader(status)
	_, err = io.CopyN(w, content4xxFile, size)
	return err
}

func hasOriginIsolation(r *http.Request) bool {
	_, subdomainGw := r.Context().Value(SubdomainHostnameKey).(string)
	_, dnslink := r.Context().Value(DNSLinkHostnameKey).(string)

	if subdomainGw || dnslink {
		return true
	}

	return false
}

// Deprecated: legacy ipfs-404.html files are superseded by _redirects file
// This is provided only for backward-compatibility, until websites migrate
// to 404s managed via _redirects file (https://github.com/ipfs/specs/pull/290)
func (i *handler) serveLegacy404IfPresent(w http.ResponseWriter, r *http.Request, imPath ImmutablePath, logger *zap.SugaredLogger) bool {
	resolved404File, ctype, err := i.searchUpTreeFor404(r, imPath)
	if err != nil {
		return false
	}
	defer resolved404File.Close()

	size, err := resolved404File.Size()
	if err != nil {
		return false
	}

	logger.Debugw("using pretty 404 file", "path", imPath)
	w.Header().Set("Content-Type", ctype)
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.WriteHeader(http.StatusNotFound)
	_, err = io.CopyN(w, resolved404File, size)
	return err == nil
}

func (i *handler) searchUpTreeFor404(r *http.Request, imPath ImmutablePath) (files.File, string, error) {
	filename404, ctype, err := preferred404Filename(r.Header.Values("Accept"))
	if err != nil {
		return nil, "", err
	}

	pathComponents := strings.Split(imPath.String(), "/")

	for idx := len(pathComponents); idx >= 3; idx-- {
		pretty404 := gopath.Join(append(pathComponents[0:idx], filename404)...)
		parsed404Path := ipath.New("/" + pretty404)
		if parsed404Path.IsValid() != nil {
			break
		}
		imparsed404Path, err := NewImmutablePath(parsed404Path)
		if err != nil {
			break
		}

		_, getResp, err := i.backend.Get(r.Context(), imparsed404Path)
		if err != nil {
			continue
		}
		if getResp.bytes == nil {
			return nil, "", fmt.Errorf("found a pretty 404 but it was not a file")
		}
		return getResp.bytes, ctype, nil
	}

	return nil, "", fmt.Errorf("no pretty 404 in any parent folder")
}

func preferred404Filename(acceptHeaders []string) (string, string, error) {
	// If we ever want to offer a 404 file for a different content type
	// then this function will need to parse q weightings, but for now
	// the presence of anything matching HTML is enough.
	for _, acceptHeader := range acceptHeaders {
		accepted := strings.Split(acceptHeader, ",")
		for _, spec := range accepted {
			contentType := strings.SplitN(spec, ";", 1)[0]
			switch contentType {
			case "*/*", "text/*", "text/html":
				return "ipfs-404.html", "text/html", nil
			}
		}
	}

	return "", "", fmt.Errorf("there is no 404 file for the requested content types")
}
