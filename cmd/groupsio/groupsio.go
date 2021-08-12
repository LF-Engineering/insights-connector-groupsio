package main

import (
	"encoding/base64"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	neturl "net/url"

	"github.com/LF-Engineering/insights-datasource-confluence/gen/models"
	shared "github.com/LF-Engineering/insights-datasource-shared"
	"github.com/go-openapi/strfmt"
	jsoniter "github.com/json-iterator/go"
)

const (
	// ConfluenceBackendVersion - backend version
	ConfluenceBackendVersion = "0.1.0"
	// ConfluenceDefaultMaxContents - max contents to fetch at a time
	ConfluenceDefaultMaxContents = 1000
	// ConfluenceDefaultSearchField - default search field
	ConfluenceDefaultSearchField = "item_id"
)

var (
	gMaxUpdatedAt    time.Time
	gMaxUpdatedAtMtx = &sync.Mutex{}
	// ConfluenceDataSource - constant
	ConfluenceDataSource = &models.DataSource{Name: "Confluence", Slug: "confluence"}
	gConfluenceMetaData  = &models.MetaData{BackendName: "confluence", BackendVersion: ConfluenceBackendVersion}
)

// DSConfluence - DS implementation for confluence - does nothing at all, just presents a skeleton code
type DSConfluence struct {
	URL             string // Confluence instance URL, for example https://wiki.lfnetworking.org
	MaxContents     int    // Defaults to ConfluenceDefaultMaxContents (1000)
	User            string // If user is provided then we assume that we don't have base64 encoded user:token yet
	Token           string // If user is not specified we assume that token already contains "<username>:<your-api-token>"
	SkipBody        bool   // Do not retrieve comments body from API and do not store it (schema allows null for body)
	FlagURL         *string
	FlagMaxContents *int
	FlagUser        *string
	FlagToken       *string
	FlagSkipBody    *bool
}

// AddFlags - add confluence specific flags
func (j *DSConfluence) AddFlags() {
	j.FlagURL = flag.String("confluence-url", "", "Confluence instance URL, for example https://wiki.lfnetworking.org")
	j.FlagMaxContents = flag.Int("confluence-max-contents", ConfluenceDefaultMaxContents, "Max Contents - defaults to ConfluenceDefaultMaxContents (1000)")
	j.FlagUser = flag.String("confluence-user", "", "User: if user is provided then we assume that we don't have base64 encoded user:token yet")
	j.FlagToken = flag.String("confluence-token", "", "Token: if user is not specified we assume that token already contains \"<username>:<your-api-token>\"")
	j.FlagSkipBody = flag.Bool("confluence-skip-body", false, "Do not retrieve comments body from API and do not store it (schema allows null for body)")
}

// ParseArgs - parse confluence specific environment variables
func (j *DSConfluence) ParseArgs(ctx *shared.Ctx) (err error) {
	// Confluence URL
	if shared.FlagPassed(ctx, "url") && *j.FlagURL != "" {
		j.URL = *j.FlagURL
	}
	if ctx.EnvSet("URL") {
		j.URL = ctx.Env("URL")
	}

	// Max contents
	passed := shared.FlagPassed(ctx, "max-contents")
	if passed {
		j.MaxContents = *j.FlagMaxContents
	}
	if ctx.EnvSet("MAX_CONTENTS") {
		maxContents, err := strconv.Atoi(ctx.Env("MAX_CONTENTS"))
		shared.FatalOnError(err)
		if maxContents > 0 {
			j.MaxContents = maxContents
		}
	} else if !passed {
		j.MaxContents = ConfluenceDefaultMaxContents
	}

	// SkipBody
	if shared.FlagPassed(ctx, "skip-body") {
		j.SkipBody = *j.FlagSkipBody
	}
	skipBody, present := ctx.BoolEnvSet("SKIP_BODY")
	if present {
		j.SkipBody = skipBody
	}

	// SSO User
	if shared.FlagPassed(ctx, "user") && *j.FlagUser != "" {
		j.User = *j.FlagUser
	}
	if ctx.EnvSet("USER") {
		j.User = ctx.Env("USER")
	}
	if j.User != "" {
		shared.AddRedacted(j.User, false)
	}

	// SSO Token
	if shared.FlagPassed(ctx, "token") && *j.FlagToken != "" {
		j.Token = *j.FlagToken
	}
	if ctx.EnvSet("TOKEN") {
		j.Token = ctx.Env("TOKEN")
	}
	if j.Token != "" {
		shared.AddRedacted(j.Token, false)
	}

	// SSO: Handle either user,token pair or just a token
	if j.User != "" {
		// If user is specified, then we must calculate base64(user:token) to get a real token
		j.Token = base64.StdEncoding.EncodeToString([]byte(j.User + ":" + j.Token))
		shared.AddRedacted(j.Token, false)
	}
	gConfluenceMetaData.Project = ctx.Project
	gConfluenceMetaData.Tags = ctx.Tags
	return
}

// Validate - is current DS configuration OK?
func (j *DSConfluence) Validate() (err error) {
	j.URL = strings.TrimSpace(j.URL)
	if strings.HasSuffix(j.URL, "/") {
		j.URL = j.URL[:len(j.URL)-1]
	}
	if j.URL == "" {
		err = fmt.Errorf("URL must be set")
	}
	return
}

// Init - initialize confluence data source
func (j *DSConfluence) Init(ctx *shared.Ctx) (err error) {
	shared.NoSSLVerify()
	ctx.InitEnv("confluence")
	j.AddFlags()
	ctx.Init()
	err = j.ParseArgs(ctx)
	if err != nil {
		return
	}
	err = j.Validate()
	if err != nil {
		return
	}
	if ctx.Debug > 1 {
		shared.Printf("confluence: %+v\nshared context: %s\n", j, ctx.Info())
	}
	return
}

// GetHistoricalContents - get historical contents from teh current content
func (j *DSConfluence) GetHistoricalContents(ctx *shared.Ctx, content map[string]interface{}, dateFrom time.Time) (contents []map[string]interface{}, err error) {
	iContentURL, _ := shared.Dig(content, []string{"_links", "webui"}, true, false)
	ancestors, ok := shared.Dig(content, []string{"ancestors"}, false, true)
	if !ok {
		ancestors = []interface{}{}
	}
	contentURL, _ := iContentURL.(string)
	contentURL = j.URL + contentURL
	content["content_url"] = contentURL
	content["ancestors"] = ancestors
	iVersionNumber, _ := shared.Dig(content, []string{"version", "number"}, true, false)
	lastVersion := int(iVersionNumber.(float64))
	if lastVersion == 1 {
		contents = append(contents, content)
		return
	}
	iID, ok := content["id"]
	if !ok {
		err = fmt.Errorf("missing id property in content: %+v", content)
		return
	}
	id, ok := iID.(string)
	if !ok {
		err = fmt.Errorf("id property is not a string: %+v", content)
		return
	}
	method := "GET"
	var headers map[string]string
	if j.Token != "" {
		headers = map[string]string{"Authorization": "Basic " + j.Token}
	}
	cacheDur := time.Duration(24) * time.Hour
	version := 1
	var (
		res    interface{}
		status int
	)
	for {
		var url string
		if j.SkipBody {
			url = j.URL + "/rest/api/content/" + id + "?version=" + strconv.Itoa(version) + "&status=historical&expand=" + neturl.QueryEscape("history,version")
		} else {
			url = j.URL + "/rest/api/content/" + id + "?version=" + strconv.Itoa(version) + "&status=historical&expand=" + neturl.QueryEscape("body.storage,history,version")
		}
		if ctx.Debug > 1 {
			shared.Printf("historical content url: %s\n", url)
		}
		res, status, _, _, err = shared.Request(
			ctx,
			url,
			method,
			headers,
			nil,
			nil,
			map[[2]int]struct{}{{200, 200}: {}}, // JSON statuses: 200
			nil,                                 // Error statuses
			map[[2]int]struct{}{{200, 200}: {}, {500, 500}: {}, {404, 404}: {}}, // OK statuses: 200
			map[[2]int]struct{}{{200, 200}: {}},                                 // Cache statuses: 200
			false,                                                               // retry
			&cacheDur,                                                           // cache duration
			false,                                                               // skip in dry-run mode
		)
		if status == 404 || status == 500 {
			if ctx.Debug > 1 {
				shared.Printf("%s: v%d status %d: %s\n", id, version, status, url)
			}
			break
		}
		if err != nil {
			return
		}
		result, ok := res.(map[string]interface{})
		if !ok {
			err = fmt.Errorf("cannot parse JSON from (status: %d):\n%s", status, string(res.([]byte)))
			return
		}
		iLatest, _ := shared.Dig(result, []string{"history", "latest"}, true, false)
		latest, ok := iLatest.(bool)
		if !ok {
			err = fmt.Errorf("cannot read latest property: %+v", result)
			return
		}
		iWhen, ok := shared.Dig(result, []string{"version", "when"}, false, true)
		if !ok {
			if ctx.Debug > 0 {
				shared.Printf("missing 'when' attribute for content %s version %d, skipping\n", id, version)
			}
			if latest {
				break
			}
			version++
			continue
		}
		var when time.Time
		when, err = shared.TimeParseInterfaceString(iWhen)
		if err != nil {
			return
		}
		if !when.Before(dateFrom) {
			result["content_url"] = contentURL
			result["ancestors"] = ancestors
			contents = append(contents, result)
		}
		if ctx.Debug > 2 {
			shared.Printf("%s: v%d %+v,%v (%s)\n", id, version, when, latest, url)
		}
		if latest {
			break
		}
		version++
		if version == lastVersion {
			break
		}
	}
	contents = append(contents, content)
	if ctx.Debug > 1 {
		shared.Printf("final %s %d (%d historical contents)\n", id, version, len(contents))
	}
	return
}

// GetConfluenceContents - get confluence historical contents
func (j *DSConfluence) GetConfluenceContents(ctx *shared.Ctx, fromDate, next string) (contents []map[string]interface{}, newNext string, err error) {
	/*
		shared.Printf("GetConfluenceContents: in\n")
		defer func() {
			shared.Printf("GetConfluenceContents: out %d\n", len(contents))
		}()
	*/
	if next == "" {
		return
	}
	method := "GET"
	var headers map[string]string
	if j.Token != "" {
		headers = map[string]string{"Authorization": "Basic " + j.Token}
	}
	cacheDur := time.Duration(24) * time.Hour
	var url string
	// Init state
	if next == "i" {
		////url = j.URL + "/rest/api/content/search?cql=" + neturl.QueryEscape("lastModified>='"+fromDate+"' order by lastModified") + fmt.Sprintf("&limit=%d&expand=ancestors", j.MaxContents)
		if j.SkipBody {
			url = j.URL + "/rest/api/content/search?cql=" + neturl.QueryEscape("lastModified>='"+fromDate+"' order by lastModified") + fmt.Sprintf("&limit=%d", j.MaxContents) + "&expand=" + neturl.QueryEscape("ancestors,version")
		} else {
			url = j.URL + "/rest/api/content/search?cql=" + neturl.QueryEscape("lastModified>='"+fromDate+"' order by lastModified") + fmt.Sprintf("&limit=%d", j.MaxContents) + "&expand=" + neturl.QueryEscape("body.storage,ancestors,version")
		}
	} else {
		url = j.URL + next
	}
	if ctx.Debug > 1 {
		shared.Printf("content url: %s\n", url)
	}
	res, status, _, _, err := shared.Request(
		ctx,
		url,
		method,
		headers,
		nil,
		nil,
		map[[2]int]struct{}{{200, 200}: {}}, // JSON statuses: 200
		nil,                                 // Error statuses
		map[[2]int]struct{}{{200, 200}: {}}, // OK statuses: 200
		map[[2]int]struct{}{{200, 200}: {}}, // Cache statuses: 200
		false,                               // retry
		&cacheDur,                           // cache duration
		false,                               // skip in dry-run mode
	)
	// shared.Printf("res=%v\n", res.(map[string]interface{}))
	// shared.Printf("status=%d, err=%v\n", status, err)
	if err != nil {
		return
	}
	result, ok := res.(map[string]interface{})
	if !ok {
		err = fmt.Errorf("cannot parse JSON from (status: %d):\n%s", status, string(res.([]byte)))
		return
	}
	iNext, ok := shared.Dig(result, []string{"_links", "next"}, false, true)
	if ok {
		newNext, _ = iNext.(string)
	}
	iResults, ok := result["results"]
	if ok {
		results, ok := iResults.([]interface{})
		if ok {
			for _, iResult := range results {
				content, ok := iResult.(map[string]interface{})
				if ok {
					contents = append(contents, content)
				}
			}
		}
	}
	return
}

// ItemID - return unique identifier for an item
func (j *DSConfluence) ItemID(item interface{}) string {
	id, _ := shared.Dig(item, []string{"id"}, true, false)
	//versionNumber, _ := shared.Dig(item, []string{"version", "number"}, true, false)
	//return id.(string) + "#v" + fmt.Sprintf("%.0f", versionNumber.(float64))
	return id.(string)
}

// ItemUpdatedOn - return updated on date for an item
func (j *DSConfluence) ItemUpdatedOn(item interface{}) time.Time {
	iWhen, _ := shared.Dig(item, []string{"version", "when"}, false, true)
	when, err := shared.TimeParseInterfaceString(iWhen)
	shared.FatalOnError(err)
	return when
}

// AddMetadata - add metadata to the item
func (j *DSConfluence) AddMetadata(ctx *shared.Ctx, item interface{}) (mItem map[string]interface{}) {
	mItem = make(map[string]interface{})
	origin := j.URL
	tags := ctx.Tags
	if len(tags) == 0 {
		tags = []string{origin}
	}
	itemID := j.ItemID(item)
	updatedOn := j.ItemUpdatedOn(item)
	uuid := shared.UUIDNonEmpty(ctx, origin, itemID)
	timestamp := time.Now()
	mItem["backend_name"] = ctx.DS
	mItem["backend_version"] = ConfluenceBackendVersion
	mItem["timestamp"] = fmt.Sprintf("%.06f", float64(timestamp.UnixNano())/1.0e9)
	mItem["uuid"] = uuid
	mItem["origin"] = origin
	mItem["tags"] = tags
	mItem["offset"] = float64(updatedOn.Unix())
	mItem["category"] = "historical content"
	mItem["search_fields"] = make(map[string]interface{})
	id, _ := shared.Dig(item, []string{"id"}, true, false)
	versionNumber, _ := shared.Dig(item, []string{"version", "number"}, true, false)
	var ancestorIDs []interface{}
	iAncestors, ok := shared.Dig(item, []string{"ancestors"}, false, true)
	if ok {
		ancestors, ok := iAncestors.([]interface{})
		if ok {
			for _, iAncestor := range ancestors {
				ancestor, ok := iAncestor.(map[string]interface{})
				if !ok {
					continue
				}
				ancestorID, ok := ancestor["id"]
				if ok {
					ancestorIDs = append(ancestorIDs, ancestorID)
				}
			}
		}
	}
	shared.FatalOnError(shared.DeepSet(mItem, []string{"search_fields", ConfluenceDefaultSearchField}, itemID, false))
	shared.FatalOnError(shared.DeepSet(mItem, []string{"search_fields", "content_id"}, id, false))
	shared.FatalOnError(shared.DeepSet(mItem, []string{"search_fields", "ancestor_ids"}, ancestorIDs, false))
	shared.FatalOnError(shared.DeepSet(mItem, []string{"search_fields", "version_number"}, versionNumber, false))
	// shared.Printf("%+v\n", mItem["search_fields"])
	mItem["metadata__updated_on"] = shared.ToESDate(updatedOn)
	mItem["metadata__timestamp"] = shared.ToESDate(timestamp)
	// mItem[ProjectSlug] = ctx.ProjectSlug
	return
}

// Sync - sync confluence data source
func (j *DSConfluence) Sync(ctx *shared.Ctx) (err error) {
	thrN := shared.GetThreadsNum(ctx)
	if ctx.DateFrom == nil {
		ctx.DateFrom = shared.GetLastUpdate(ctx, j.URL)
	}
	if ctx.DateFrom != nil {
		shared.Printf("%s resuming from %v\n", j.URL, ctx.DateFrom)
	}
	if ctx.DateTo != nil {
		shared.Printf("%s fetching till %v\n", j.URL, ctx.DateTo)
	}
	var (
		sDateFrom string
		dateFrom  time.Time
	)
	if ctx.DateFrom != nil {
		dateFrom = *ctx.DateFrom
		sDateFrom = shared.ToYMDHMDate(dateFrom)
	} else {
		dateFrom = shared.DefaultDateFrom
		sDateFrom = "1970-01-01 00:00"
	}
	next := "i"
	var (
		ch             chan error
		allDocs        []interface{}
		allContents    []interface{}
		allContentsMtx *sync.Mutex
		escha          []chan error
		eschaMtx       *sync.Mutex
	)
	if thrN > 1 {
		ch = make(chan error)
		allContentsMtx = &sync.Mutex{}
		eschaMtx = &sync.Mutex{}
	}
	nThreads := 0
	processContent := func(c chan error, content map[string]interface{}) (wch chan error, e error) {
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		// shared.Printf("processContent: in\n")
		var contents []map[string]interface{}
		contents, e = j.GetHistoricalContents(ctx, content, dateFrom)
		if e != nil {
			return
		}
		var esItems []interface{}
		for _, content := range contents {
			esItem := j.AddMetadata(ctx, content)
			if ctx.Project != "" {
				content["project"] = ctx.Project
			}
			esItem["data"] = content
			esItems = append(esItems, esItem)
		}
		// shared.Printf("processContent: out %d\n", len(contents))
		if allContentsMtx != nil {
			allContentsMtx.Lock()
		}
		allContents = append(allContents, esItems...)
		nContents := len(allContents)
		if nContents >= ctx.PackSize {
			sendToQueue := func(c chan error) (ee error) {
				defer func() {
					if c != nil {
						c <- ee
					}
				}()
				ee = j.ConfluenceEnrichItems(ctx, thrN, allContents, &allDocs, false)
				if ee != nil {
					shared.Printf("error %v sending %d historical contents to queue\n", ee, len(allContents))
				}
				allContents = []interface{}{}
				if allContentsMtx != nil {
					allContentsMtx.Unlock()
				}
				return
			}
			if thrN > 1 {
				wch = make(chan error)
				go func() {
					_ = sendToQueue(wch)
				}()
			} else {
				e = sendToQueue(nil)
				if e != nil {
					return
				}
			}
		} else {
			if allContentsMtx != nil {
				allContentsMtx.Unlock()
			}
		}
		return
	}
	if thrN > 1 {
		for {
			var contents []map[string]interface{}
			contents, next, err = j.GetConfluenceContents(ctx, sDateFrom, next)
			if err != nil {
				return
			}
			for _, cont := range contents {
				go func(content map[string]interface{}) {
					var (
						e    error
						esch chan error
					)
					esch, e = processContent(ch, content)
					if e != nil {
						shared.Printf("process error: %v\n", e)
						return
					}
					if esch != nil {
						if eschaMtx != nil {
							eschaMtx.Lock()
						}
						escha = append(escha, esch)
						if eschaMtx != nil {
							eschaMtx.Unlock()
						}
					}
				}(cont)
				nThreads++
				if nThreads == thrN {
					err = <-ch
					if err != nil {
						return
					}
					nThreads--
				}
			}
			if next == "" {
				break
			}
		}
		for nThreads > 0 {
			err = <-ch
			nThreads--
			if err != nil {
				return
			}
		}
	} else {
		for {
			var contents []map[string]interface{}
			contents, next, err = j.GetConfluenceContents(ctx, sDateFrom, next)
			if err != nil {
				return
			}
			for _, content := range contents {
				_, err = processContent(nil, content)
				if err != nil {
					return
				}
			}
			if next == "" {
				break
			}
		}
	}
	if eschaMtx != nil {
		eschaMtx.Lock()
	}
	for _, esch := range escha {
		err = <-esch
		if err != nil {
			if eschaMtx != nil {
				eschaMtx.Unlock()
			}
			return
		}
	}
	if eschaMtx != nil {
		eschaMtx.Unlock()
	}
	nContents := len(allContents)
	if ctx.Debug > 0 {
		shared.Printf("%d remaining contents to send to queue\n", nContents)
	}
	err = j.ConfluenceEnrichItems(ctx, thrN, allContents, &allDocs, true)
	if err != nil {
		shared.Printf("Error %v sending %d contents to queue\n", err, len(allContents))
	}
	gMaxUpdatedAtMtx.Lock()
	defer gMaxUpdatedAtMtx.Unlock()
	shared.SetLastUpdate(ctx, j.URL, gMaxUpdatedAt)
	return
}

// EnrichItem - return rich item from raw item for a given author type
func (j *DSConfluence) EnrichItem(ctx *shared.Ctx, item map[string]interface{}) (rich map[string]interface{}, err error) {
	// shared.Printf("raw: %+v\n", item)
	/*
		shared.Printf("raw: %s\n", shared.InterfaceToStringTrunc(item, shared.MaxPayloadPrintfLen, true))
		jsonBytes, err := jsoniter.Marshal(item)
		if err != nil {
			shared.Printf("Error: %+v\n", err)
			return
		}
		shared.Printf("%s\n", string(jsonBytes))
	*/
	rich = make(map[string]interface{})
	for _, field := range shared.RawFields {
		v, _ := item[field]
		rich[field] = v
	}
	page, ok := item["data"].(map[string]interface{})
	if !ok {
		err = fmt.Errorf("missing data field in item %+v", shared.DumpKeys(item))
		return
	}
	for _, field := range []string{"type", "id", "status", "title", "content_url"} {
		rich[field], _ = page[field]
	}
	title := ""
	iTitle, ok := page["title"]
	if ok {
		title, _ = iTitle.(string)
	}
	if len(title) > shared.KeywordMaxlength {
		title = title[:shared.KeywordMaxlength]
	}
	rich["title"] = title
	version, ok := page["version"].(map[string]interface{})
	if !ok {
		err = fmt.Errorf("missing version field in item %+v", shared.DumpKeys(page))
		return
	}
	user, _ := shared.Dig(version, []string{"by"}, true, false)
	rich["by"] = user
	rich["message"], _ = shared.Dig(version, []string{"message"}, false, true)
	iVersion, _ := version["number"]
	rich["version"] = iVersion
	rich["date"], _ = version["when"]
	////base, _ := shared.Dig(page, []string{"_links", "base"}, true, false)
	webUI, _ := shared.Dig(page, []string{"_links", "webui"}, true, false)
	////rich["url"] = base.(string) + webUI.(string)
	rich["url"] = j.URL + webUI.(string)
	iSpace, ok := shared.Dig(page, []string{"_expandable", "space"}, false, true)
	if ok {
		space, _ := iSpace.(string)
		space = strings.Replace(space, "/rest/api/space/", "", -1)
		rich["space"] = space
	}
	var (
		ancestorIDs    []interface{}
		ancestorTitles []interface{}
		ancestorLinks  []interface{}
	)
	iAncestors, ok := shared.Dig(page, []string{"ancestors"}, false, true)
	if ok {
		ancestors, ok := iAncestors.([]interface{})
		if ok {
			for _, iAncestor := range ancestors {
				ancestor, ok := iAncestor.(map[string]interface{})
				if !ok {
					continue
				}
				ancestorID, ok := ancestor["id"]
				ancestorIDs = append(ancestorIDs, ancestorID)
				ancestorTitle, ok := ancestor["title"]
				if ok {
					ancestorTitles = append(ancestorTitles, ancestorTitle)
				} else {
					ancestorTitles = append(ancestorTitles, "NO_TITLE")
				}
				ancestorLink, _ := shared.Dig(ancestor, []string{"_links", "webui"}, true, false)
				sAncestorLink, _ := ancestorLink.(string)
				sAncestorLink = j.URL + sAncestorLink
				ancestorLinks = append(ancestorLinks, sAncestorLink)
			}
		}
	}
	rich["ancestors_ids"] = ancestorIDs
	rich["ancestors_titles"] = ancestorTitles
	rich["ancestors_links"] = ancestorLinks
	iType, _ := shared.Dig(page, []string{"type"}, true, false)
	if iType.(string) == "page" && int(iVersion.(float64)) == 1 {
		rich["type"] = "new_page"
	}
	rich["original_type"] = iType
	rich["is_blogpost"] = 0
	tp, _ := rich["type"].(string)
	rich["is_"+tp] = 1
	// can also be rich["date"]
	updatedOn, _ := shared.Dig(item, []string{"metadata__updated_on"}, true, false)
	rich["updated_on"] = updatedOn
	if !j.SkipBody {
		iBody, ok := shared.Dig(page, []string{"body", "storage", "value"}, false, true)
		if ok {
			body, _ := iBody.(string)
			if len(body) > shared.MaxBodyLength {
				body = body[:shared.MaxBodyLength]
			}
			rich["body"] = body
		}
	}
	iAvatar, ok := shared.Dig(page, []string{"version", "by", "profilePicture", "path"}, false, true)
	if ok {
		avatar, _ := iAvatar.(string)
		rich["avatar"] = j.URL + avatar
	}
	rich["by_name"], rich["by_username"], rich["by_email"] = j.GetRoleIdentity(item)
	// From shared
	rich["metadata__enriched_on"] = time.Now()
	// rich[ProjectSlug] = ctx.ProjectSlug
	// rich["groups"] = ctx.Groups
	return
}

// GetRoleIdentity - return identity data
func (j *DSConfluence) GetRoleIdentity(item map[string]interface{}) (name, username, email string) {
	iUser, ok := shared.Dig(item, []string{"data", "version", "by"}, true, false)
	user, _ := iUser.(map[string]interface{})
	iUserName, ok := user["username"]
	if ok {
		username, _ = iUserName.(string)
	} else {
		iPublicName, ok := user["publicName"]
		if ok {
			username, _ = iPublicName.(string)
		}
	}
	iDisplayName, ok := user["displayName"]
	if ok {
		name, _ = iDisplayName.(string)
	}
	iEmail, ok := user["email"]
	if ok {
		email, _ = iEmail.(string)
	}
	return
}

// GetModelData - return data in swagger format
func (j *DSConfluence) GetModelData(ctx *shared.Ctx, docs []interface{}) (data *models.Data) {
	data = &models.Data{
		DataSource: ConfluenceDataSource,
		MetaData:   gConfluenceMetaData,
	}
	source := data.DataSource.Slug
	for _, iDoc := range docs {
		var (
			createdAt time.Time
			body      *string
			space     string
			ancestors []*models.Ancestor
		)
		doc, _ := iDoc.(map[string]interface{})
		// shared.Printf("rich %+v\n", doc)
		typ, _ := doc["type"].(string)
		typ = "confluence_" + typ
		origType, _ := doc["original_type"].(string)
		origType = "confluence_" + origType
		iUpdatedOn, _ := doc["updated_on"]
		updatedOn, err := shared.TimeParseInterfaceString(iUpdatedOn)
		shared.FatalOnError(err)
		if typ == "confluence_new_page" {
			createdAt = updatedOn
		}
		gMaxUpdatedAtMtx.Lock()
		if updatedOn.After(gMaxUpdatedAt) {
			gMaxUpdatedAt = updatedOn
		}
		gMaxUpdatedAtMtx.Unlock()
		docUUID, _ := doc["uuid"].(string)
		actUUID := shared.UUIDNonEmpty(ctx, docUUID, shared.ToESDate(updatedOn))
		if !j.SkipBody {
			sBody, okBody := doc["body"].(string)
			if okBody {
				body = &sBody
			}
		}
		avatar, _ := doc["avatar"].(string)
		internalID, _ := doc["id"].(string)
		title, _ := doc["title"].(string)
		url, _ := doc["url"].(string)
		space, _ = doc["space"].(string)
		version, _ := doc["version"].(float64)
		name, _ := doc["by_name"].(string)
		username, _ := doc["by_username"].(string)
		email, _ := doc["by_email"].(string)
		name, username = shared.PostprocessNameUsername(name, username, email)
		userUUID := shared.UUIDAffs(ctx, source, email, name, username)
		iAIDs, ok := doc["ancestors_ids"]
		if ok {
			aIDs, ok := iAIDs.([]interface{})
			if ok {
				iATitles, _ := doc["ancestors_titles"]
				iALinks, _ := doc["ancestors_links"]
				aTitles, _ := iATitles.([]interface{})
				aLinks, _ := iALinks.([]interface{})
				for i, aID := range aIDs {
					aid, _ := aID.(string)
					aTitle, _ := aTitles[i].(string)
					aLink, _ := aLinks[i].(string)
					ancestors = append(ancestors, &models.Ancestor{
						InternalID: aid,
						Title:      aTitle,
						URL:        aLink,
					})
				}
			}
		}
		event := &models.Event{
			DocumentActivity: &models.DocumentActivity{
				DocumentActivityType: typ,
				CreatedAt:            strfmt.DateTime(updatedOn),
				ID:                   actUUID,
				Body:                 body,
				Identity: &models.Identity{
					ID:           userUUID,
					AvatarURL:    avatar,
					DataSourceID: source,
					Name:         name,
					Username:     username,
					Email:        email,
				},
				Documentation: &models.Documentation{
					ID:              docUUID,
					InternalID:      internalID,
					CreatedAt:       strfmt.DateTime(createdAt),
					UpdatedAt:       strfmt.DateTime(updatedOn), // this is only set for confluence_new_page
					Title:           title,
					URL:             url,
					Space:           &space,
					DataSourceID:    source,
					DocumentType:    origType,
					DocumentVersion: fmt.Sprintf("%.0f", version),
					Slug:            ctx.Project, // FIXME: We point to a project specified from the configuration - but aren't given documentation pages already connected to projects via onboarding?
					Ancestors:       ancestors,
				},
			},
		}
		data.Events = append(data.Events, event)
	}
	return
}

// ConfluenceEnrichItems - iterate items and enrich them
// items is a current pack of input items
// docs is a pointer to where extracted identities will be stored
func (j *DSConfluence) ConfluenceEnrichItems(ctx *shared.Ctx, thrN int, items []interface{}, docs *[]interface{}, final bool) (err error) {
	shared.Printf("input processing(%d/%d/%v)\n", len(items), len(*docs), final)
	outputDocs := func() {
		if len(*docs) > 0 {
			// actual output
			shared.Printf("output processing(%d/%d/%v)\n", len(items), len(*docs), final)
			data := j.GetModelData(ctx, *docs)
			// FIXME: actual output to some consumer...
			jsonBytes, err := jsoniter.Marshal(data)
			if err != nil {
				shared.Printf("Error: %+v\n", err)
				return
			}
			shared.Printf("%s\n", string(jsonBytes))
			*docs = []interface{}{}
			gMaxUpdatedAtMtx.Lock()
			defer gMaxUpdatedAtMtx.Unlock()
			shared.SetLastUpdate(ctx, j.URL, gMaxUpdatedAt)
		}
	}
	if final {
		defer func() {
			outputDocs()
		}()
	}
	if ctx.Debug > 0 {
		shared.Printf("confluence enrich items %d\n", len(items))
	}
	var (
		mtx *sync.RWMutex
		ch  chan error
	)
	if thrN > 1 {
		mtx = &sync.RWMutex{}
		ch = make(chan error)
	}
	nThreads := 0
	procItem := func(c chan error, idx int) (e error) {
		if thrN > 1 {
			mtx.RLock()
		}
		src := items[idx]
		if thrN > 1 {
			mtx.RUnlock()
		}
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		doc, ok := src.(map[string]interface{})
		if !ok {
			e = fmt.Errorf("Failed to parse document %+v", doc)
			return
		}
		var rich map[string]interface{}
		rich, e = j.EnrichItem(ctx, doc)
		if e != nil {
			return
		}
		if thrN > 1 {
			mtx.Lock()
		}
		*docs = append(*docs, rich)
		if len(*docs) >= ctx.PackSize {
			outputDocs()
		}
		if thrN > 1 {
			mtx.Unlock()
		}
		return
	}
	if thrN > 1 {
		for i := range items {
			go func(i int) {
				_ = procItem(ch, i)
			}(i)
			nThreads++
			if nThreads == thrN {
				err = <-ch
				if err != nil {
					return
				}
				nThreads--
			}
		}
		for nThreads > 0 {
			err = <-ch
			nThreads--
			if err != nil {
				return
			}
		}
		return
	}
	for i := range items {
		err = procItem(nil, i)
		if err != nil {
			return
		}
	}
	return
}

func main() {
	var (
		ctx        shared.Ctx
		confluence DSConfluence
	)
	err := confluence.Init(&ctx)
	if err != nil {
		shared.Printf("Error: %+v\n", err)
		return
	}
	err = confluence.Sync(&ctx)
	if err != nil {
		shared.Printf("Error: %+v\n", err)
		return
	}
}
