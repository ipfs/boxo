package main

import (
	"embed"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/ipfs/boxo/gateway/assets"
	"github.com/ipfs/go-cid"

	// Ensure basic codecs are registered.
	_ "github.com/ipld/go-ipld-prime/codec/cbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	_ "github.com/ipld/go-ipld-prime/codec/json"
	_ "github.com/ipld/go-ipld-prime/codec/raw"
	"github.com/ipld/go-ipld-prime/multicodec"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	mc "github.com/multiformats/go-multicodec"
)

//go:embed dag/*.block
var embeds embed.FS

const (
	testPath = "/ipfs/QmFooBarQXB2mzChmMeKY47C43LxUdg1NDJ5MWcKMKxDu7/a/b/c"
)

var directoryTestData = assets.DirectoryTemplateData{
	GlobalData: assets.GlobalData{
		Menu: []assets.MenuItem{{
			URL:   "http://example.com",
			Title: "Support",
		}},
		GatewayURL: "//localhost:3000",
		DNSLink:    true,
	},
	Listing: []assets.DirectoryItem{{
		Size:      "25 MiB",
		Name:      "short-film.mov",
		Path:      testPath + "/short-film.mov",
		Hash:      "QmbWqxBEKC3P8tqsKc98xmWNzrzDtRLMiMPL8wBuTGsMnR",
		ShortHash: "QmbW\u2026sMnR",
	}, {
		Size:      "23 KiB",
		Name:      "250pxيوسف_الوزاني_صورة_ملتقطة_بواسطة_مرصد_هابل_الفضائي_توضح_سديم_السرطان،_وهو_بقايا_مستعر_أعظم._.jpg",
		Path:      testPath + "/250pxيوسف_الوزاني_صورة_ملتقطة_بواسطة_مرصد_هابل_الفضائي_توضح_سديم_السرطان،_وهو_بقايا_مستعر_أعظم._.jpg",
		Hash:      "QmUwrKrMTrNv8QjWGKMMH5QV9FMPUtRCoQ6zxTdgxATQW6",
		ShortHash: "QmUw\u2026TQW6",
	}, {
		Size:      "1 KiB",
		Name:      "this-piece-of-papers-got-47-words-37-sentences-58-words-we-wanna-know.txt",
		Path:      testPath + "/this-piece-of-papers-got-47-words-37-sentences-58-words-we-wanna-know.txt",
		Hash:      "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
		ShortHash: "bafy\u2026bzdi",
	}},
	Size: "25 MiB",
	Path: testPath,
	Breadcrumbs: []assets.Breadcrumb{{
		Name: "ipfs",
	}, {
		Name: "QmFooBarQXB2mzChmMeKY47C43LxUdg1NDJ5MWcKMKxDu7",
		Path: testPath + "/../../..",
	}, {
		Name: "a",
		Path: testPath + "/../..",
	}, {
		Name: "b",
		Path: testPath + "/..",
	}, {
		Name: "c",
		Path: testPath,
	}},
	BackLink: testPath + "/..",
	Hash:     "QmFooBazBar2mzChmMeKY47C43LxUdg1NDJ5MWcKMKxDu7",
}

var dagTestData = map[string]*assets.DagTemplateData{}

func loadDagTestData() {
	entries, err := embeds.ReadDir("dag")
	if err != nil {
		panic(err)
	}

	for _, entry := range entries {
		cidStr := strings.TrimSuffix(entry.Name(), ".block")
		cid, err := cid.Decode(cidStr)
		if err != nil {
			panic(err)
		}

		f, err := embeds.Open("dag/" + entry.Name())
		if err != nil {
			panic(err)
		}

		codec := cid.Prefix().Codec
		decoder, err := multicodec.LookupDecoder(codec)
		if err != nil {
			panic(err)
		}

		node := basicnode.Prototype.Any.NewBuilder()
		err = decoder(node, f)
		if err != nil {
			panic(err)
		}

		cidCodec := mc.Code(cid.Prefix().Codec)

		dag, err := assets.ParseNode(node.Build())
		if err != nil {
			panic(err)
		}

		dagTestData[cid.String()] = &assets.DagTemplateData{
			GlobalData: assets.GlobalData{
				Menu: []assets.MenuItem{{
					URL:   "http://example.com",
					Title: "Support",
				}},
				GatewayURL: "//localhost:3000",
				DNSLink:    true,
			},
			Path:      "/ipfs/" + cid.String(),
			CID:       cid.String(),
			CodecName: cidCodec.String(),
			CodecHex:  fmt.Sprintf("0x%x", uint64(cidCodec)),
			Node:      dag,
		}
	}
}

func init() {
	// Append all types so we can preview the icons for all file types.
	for ext := range assets.KnownIcons {
		directoryTestData.Listing = append(directoryTestData.Listing, assets.DirectoryItem{
			Size:      "1 MiB",
			Name:      "file" + ext,
			Path:      testPath + "/" + "file" + ext,
			Hash:      "QmbWqxBEKC3P8tqsKc98xmWNzrzDtRLMiMPL8wBuTGsMnR",
			ShortHash: "QmbW\u2026sMnR",
		})
	}

	loadDagTestData()
}

func runTemplate(w http.ResponseWriter, filename string, data interface{}) {
	fs := os.DirFS(".")
	tpl, err := assets.BuildTemplate(fs, filename)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to parse template file: %s", err), http.StatusInternalServerError)
		return
	}
	err = tpl.Execute(w, data)
	if err != nil {
		_, _ = w.Write([]byte(fmt.Sprintf("error during body generation: %v", err)))
	}
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dag":
			cid := r.URL.Query().Get("cid")
			if cid == "" {
				cid = "bafyreiaocls5bt2ha5vszv5pwz34zzcdf3axk3uqa56bgsgvlkbezw67hq"
			}

			runTemplate(w, "dag.html", dagTestData[cid])
		case "/directory":
			runTemplate(w, "directory.html", directoryTestData)
		case "/error":
			statusCode, err := strconv.Atoi(r.URL.Query().Get("code"))
			if err != nil {
				statusCode = 500
			}
			runTemplate(w, "error.html", &assets.ErrorTemplateData{
				GlobalData: assets.GlobalData{
					Menu: []assets.MenuItem{{
						URL:   "http://example.com",
						Title: "Support",
					}},
				},
				StatusCode: statusCode,
				StatusText: http.StatusText(statusCode),
				Error:      "this is the verbatim error: lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua",
			})
		case "/":
			html := `<p>Test paths: <a href="/dag">DAG</a>, <a href="/directory">Directory</a>, <a href="/error?code=500">Error</a>.`
			_, _ = w.Write([]byte(html))
		default:
			http.Redirect(w, r, "/", http.StatusSeeOther)
		}
	})

	fmt.Printf("listening on http://localhost:3000/\n")
	_ = http.ListenAndServe("localhost:3000", mux)
}
