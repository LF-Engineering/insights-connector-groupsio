package main

import (
	"archive/zip"
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	neturl "net/url"

	"github.com/LF-Engineering/insights-datasource-groupsio/gen/models"
	shared "github.com/LF-Engineering/insights-datasource-shared"
	"github.com/go-openapi/strfmt"
	jsoniter "github.com/json-iterator/go"
)

const (
	// GroupsioBackendVersion - backend version
	GroupsioBackendVersion = "0.1.0"
	// GroupsioURLRoot - root url for group name origin
	GroupsioURLRoot = "https://groups.io/g/"
	// GroupsioAPIURL - Groups.io API URL
	GroupsioAPIURL = "https://groups.io/api/v1"
	// GroupsioAPILogin - login API
	GroupsioAPILogin = "/login"
	// GroupsioAPIGetsubs - getsubs API
	GroupsioAPIGetsubs = "/getsubs"
	// GroupsioAPIDownloadArchives - download archives API
	GroupsioAPIDownloadArchives = "/downloadarchives"
	// GroupsioDefaultArchPath - default path where archives are stored
	GroupsioDefaultArchPath = "/tmp/mailinglists"
	// GroupsioMBoxFile - default messages file name
	GroupsioMBoxFile = "messages.zip"
	// GroupsioDefaultSearchField - default search field
	GroupsioDefaultSearchField = "item_id"
	// GroupsioMaxRichMessageLines - maximum number of message text/plain lines copied to rich index
	GroupsioMaxRichMessageLines = 100
	// GroupsioMaxRecipients - maximum number of emails parsed from To:
	GroupsioMaxRecipients = 100
	// GroupsIO - used as an index
	GroupsIO = "groupsio"
)

var (
	gMaxCreatedAt    time.Time
	gMaxCreatedAtMtx = &sync.Mutex{}
	// GroupsioDataSource - constant
	GroupsioDataSource = &models.DataSource{Name: "Groups.io", Slug: GroupsIO}
	gGroupsioMetaData  = &models.MetaData{BackendName: GroupsIO, BackendVersion: GroupsioBackendVersion}
)

// DSGroupsio - DS implementation for stub - does nothing at all, just presents a skeleton code
type DSGroupsio struct {
	GroupName     string // Group name like group+topic
	Email         string // Email to access group
	Password      string // Password to access group
	SaveArch      bool   // Do we want to save archives locally?
	ArchPath      string // Archives path - default GroupsioDefaultArchPath
	FlagGroupName *string
	FlagEmail     *string
	FlagPassword  *string
	FlagSaveArch  *bool
	FlagArchPath  *string
	GroupID       int64
}

// AddFlags - add groups.io specific flags
func (j *DSGroupsio) AddFlags() {
	j.FlagGroupName = flag.String("groupsio-group-name", "", "groups.io group name like for example hyperledger+fabric")
	j.FlagEmail = flag.String("groupsio-email", "", "Email to access group")
	j.FlagPassword = flag.String("groupsio-password", "", "Password to access group")
	j.FlagSaveArch = flag.Bool("groupsio-save-archives", false, "Do we want to save archives locally?")
	j.FlagArchPath = flag.String("groupsio-archives-path", GroupsioDefaultArchPath, "Archives path - default "+GroupsioDefaultArchPath)
}

// ParseArgs - parse stub specific environment variables
func (j *DSGroupsio) ParseArgs(ctx *shared.Ctx) (err error) {
	// Confluence URL
	if shared.FlagPassed(ctx, "group-name") && *j.FlagGroupName != "" {
		j.GroupName = *j.FlagGroupName
	}
	if ctx.EnvSet("GROUP_NAME") {
		j.GroupName = ctx.Env("GROUP_NAME")
	}
	// Email
	if shared.FlagPassed(ctx, "email") && *j.FlagEmail != "" {
		j.Email = *j.FlagEmail
	}
	if ctx.EnvSet("EMAIL") {
		j.Email = ctx.Env("EMAIL")
	}
	if j.Email != "" {
		shared.AddRedacted(j.Email, false)
		shared.AddRedacted(neturl.QueryEscape(j.Email), false)
	}

	// Password
	if shared.FlagPassed(ctx, "password") && *j.FlagPassword != "" {
		j.Password = *j.FlagPassword
	}
	if ctx.EnvSet("PASSWORD") {
		j.Password = ctx.Env("PASSWORD")
	}
	if j.Password != "" {
		shared.AddRedacted(j.Password, false)
		shared.AddRedacted(neturl.QueryEscape(j.Password), false)
	}

	// Save archives
	if shared.FlagPassed(ctx, "save-archives") {
		j.SaveArch = *j.FlagSaveArch
	}
	saveArch, present := ctx.BoolEnvSet("SAVE_ARCHIVES")
	if present {
		j.SaveArch = saveArch
	}

	// Archives path
	j.ArchPath = GroupsioDefaultArchPath
	if shared.FlagPassed(ctx, "archives-path") && *j.FlagArchPath != "" {
		j.ArchPath = *j.FlagArchPath
	}
	if ctx.EnvSet("ARCHIVES_PATH") {
		j.ArchPath = ctx.Env("ARCHIVES_PATH")
	}

	// NOTE: don't forget this
	gGroupsioMetaData.Project = ctx.Project
	gGroupsioMetaData.Tags = ctx.Tags
	return
}

// Validate - is current DS configuration OK?
func (j *DSGroupsio) Validate() (err error) {
	url := strings.TrimSpace(j.GroupName)
	if strings.HasSuffix(url, "/") {
		url = url[:len(url)-1]
	}
	ary := strings.Split(url, "/")
	j.GroupName = ary[len(ary)-1]
	if j.GroupName == "" {
		err = fmt.Errorf("Group name must be set: [https://groups.io/g/]GROUP+channel")
		return
	}
	if j.Email == "" || j.Password == "" {
		err = fmt.Errorf("Email and Password must be set")
		return
	}
	j.ArchPath = os.ExpandEnv(j.ArchPath)
	if strings.HasSuffix(j.ArchPath, "/") {
		j.ArchPath = j.ArchPath[:len(j.ArchPath)-1]
	}
	return
}

// ItemID - return unique identifier for an item
func (j *DSGroupsio) ItemID(item interface{}) string {
	id, ok := item.(map[string]interface{})[shared.MessageIDField[GroupsIO]].(string)
	if !ok {
		shared.Fatalf("%s: ItemID() - cannot extract %s from %+v", j.GroupName, shared.MessageIDField[GroupsIO], shared.DumpKeys(item))
	}
	return id
}

// ItemUpdatedOn - return updated on date for an item
func (j *DSGroupsio) ItemUpdatedOn(item interface{}) time.Time {
	iUpdated, _ := shared.Dig(item, []string{shared.MessageDateField[GroupsIO]}, true, false)
	updated, ok := iUpdated.(time.Time)
	if !ok {
		shared.Fatalf("%s: ItemUpdatedOn() - cannot extract %s from %+v", j.GroupName, shared.MessageDateField[GroupsIO], shared.DumpKeys(item))
	}
	return updated
}

// AddMetadata - add metadata to the item
func (j *DSGroupsio) AddMetadata(ctx *shared.Ctx, msg interface{}) (mItem map[string]interface{}) {
	mItem = make(map[string]interface{})
	origin := GroupsioURLRoot + j.GroupName
	tags := ctx.Tags
	if len(tags) == 0 {
		tags = []string{origin}
	}
	msgID := j.ItemID(msg)
	updatedOn := j.ItemUpdatedOn(msg)
	uuid := shared.UUIDNonEmpty(ctx, origin, msgID)
	timestamp := time.Now()
	mItem["backend_name"] = ctx.DS
	mItem["backend_version"] = GroupsioBackendVersion
	mItem["timestamp"] = fmt.Sprintf("%.06f", float64(timestamp.UnixNano())/1.0e9)
	mItem["uuid"] = uuid
	mItem["origin"] = origin
	mItem["tags"] = tags
	mItem["offset"] = float64(updatedOn.Unix())
	mItem["category"] = "message"
	mItem["search_fields"] = make(map[string]interface{})
	shared.FatalOnError(shared.DeepSet(mItem, []string{"search_fields", GroupsioDefaultSearchField}, msgID, false))
	shared.FatalOnError(shared.DeepSet(mItem, []string{"search_fields", "group_name"}, j.GroupName, false))
	mItem["metadata__updated_on"] = shared.ToESDate(updatedOn)
	mItem["metadata__timestamp"] = shared.ToESDate(timestamp)
	// mItem[ProjectSlug] = ctx.ProjectSlug
	if ctx.Debug > 1 {
		shared.Printf("%s: %s: %v %v\n", origin, uuid, msgID, updatedOn)
	}
	return
}

// Init - initialize groups.io data source
func (j *DSGroupsio) Init(ctx *shared.Ctx) (err error) {
	shared.NoSSLVerify()
	ctx.InitEnv("Groups.io")
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
		shared.Printf("groups.io: %+v\nshared context: %s\n", j, ctx.Info())
	}
	return
}

// Sync - sync groups.io data source
func (j *DSGroupsio) Sync(ctx *shared.Ctx) (err error) {
	thrN := shared.GetThreadsNum(ctx)
	if ctx.DateFrom == nil {
		ctx.DateFrom = shared.GetLastUpdate(ctx, j.GroupName)
	}
	if ctx.DateFrom != nil {
		shared.Printf("%s resuming from %v (%d threads)\n", j.GroupName, ctx.DateFrom, thrN)
	}
	if ctx.DateTo != nil {
		shared.Printf("%s fetching till %v (%d threads)\n", j.GroupName, ctx.DateTo, thrN)
	}
	// NOTE: Non-generic starts here
	var dirPath string
	if j.SaveArch {
		dirPath = j.ArchPath + "/" + GroupsioURLRoot + j.GroupName
		dirPath, err = shared.EnsurePath(dirPath, false)
		shared.FatalOnError(err)
		shared.Printf("path to store mailing archives: %s\n", dirPath)
	} else {
		shared.Printf("processing erchives in memory, archive file not saved\n")
	}
	// Login to groups.io
	method := "GET"
	url := GroupsioAPIURL + GroupsioAPILogin + `?email=` + neturl.QueryEscape(j.Email) + `&password=` + neturl.QueryEscape(j.Password)
	// headers := map[string]string{"Content-Type": "application/json"}
	// By checking cookie expiration data I know that I can (probably) cache this even for 14 days
	// In that case other dads groupsio instances will reuse login data from L2 cache :-D
	// But we cache for 24:05 hours at most, because new subscriptions are added
	cacheLoginDur := time.Duration(24)*time.Hour + time.Duration(5)*time.Minute
	var res interface{}
	var cookies []string
	shared.Printf("groupsio login via: %s\n", url)
	res, _, cookies, _, err = shared.Request(
		ctx,
		url,
		method,
		nil,
		[]byte{},
		[]string{},                          // cookies
		nil,                                 // JSON statuses
		nil,                                 // Error statuses
		map[[2]int]struct{}{{200, 200}: {}}, // OK statuses: 200
		map[[2]int]struct{}{{200, 200}: {}}, // Cache statuses: 200
		false,                               // retry
		&cacheLoginDur,                      // cache duration
		false,                               // skip in dry-run mode
	)
	if err != nil {
		return
	}
	type Result struct {
		User struct {
			Token string `json:"csrf_token"`
			Subs  []struct {
				GroupID   int64  `json:"group_id"`
				GroupName string `json:"group_name"`
				Perms     struct {
					DownloadArchives bool `json:"download_archives"`
				} `json:"perms"`
			} `json:"subscriptions"`
		} `json:"user"`
	}
	var result Result
	err = jsoniter.Unmarshal(res.([]byte), &result)
	if err != nil {
		shared.Printf("Cannot unmarshal result from %s\n", string(res.([]byte)))
		return
	}
	groupID := int64(-1)
	for _, sub := range result.User.Subs {
		if sub.GroupName == j.GroupName {
			if !sub.Perms.DownloadArchives {
				shared.Fatalf("download archives not enabled on %s (group id %d)\n", sub.GroupName, sub.GroupID)
				return
			}
			groupID = sub.GroupID
			break
		}
	}
	if groupID < 0 {
		subs := []string{}
		dls := []string{}
		for _, sub := range result.User.Subs {
			subs = append(subs, sub.GroupName)
			if sub.Perms.DownloadArchives {
				dls = append(dls, sub.GroupName)
			}
		}
		sort.Strings(subs)
		sort.Strings(dls)
		shared.Fatalf("you are not subscribed to %s, your subscriptions(%d): %s\ndownload allowed for(%d): %s", j.GroupName, len(subs), strings.Join(subs, ", "), len(dls), strings.Join(dls, ", "))
		return
	}
	j.GroupID = groupID
	shared.Printf("%s found group ID %d\n", j.GroupName, j.GroupID)
	// We do have cookies now (from either real request or from the L2 cache)
	//url := GroupsioAPIURL + GroupsioAPILogin + `?email=` + neturl.QueryEscape(j.Email) + `&password=` + neturl.QueryEscape(j.Password)
	url = GroupsioAPIURL + GroupsioAPIDownloadArchives + `?group_id=` + fmt.Sprintf("%d", groupID)
	var (
		from   time.Time
		status int
	)
	if ctx.DateFrom != nil {
		from = *ctx.DateFrom
		from = from.Add(-1 * time.Second)
	} else {
		from = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	url += `&start_time=` + neturl.QueryEscape(shared.ToYMDTHMSZDate(from))
	shared.Printf("fetching messages from: %s\n", url)
	// Groups.io blocks downloading archives more often than 24 hours
	cacheMsgDur := time.Duration(24)*time.Hour + time.Duration(5)*time.Minute
	res, status, _, _, err = shared.Request(
		ctx,
		url,
		method,
		nil,
		[]byte{},
		cookies,
		nil,                                 // JSON statuses
		nil,                                 // Error statuses
		map[[2]int]struct{}{{200, 200}: {}}, // OK statuses: 200
		map[[2]int]struct{}{{200, 200}: {}}, // Cache statuses: 200
		false,                               // retry
		&cacheMsgDur,                        // cache duration
		false,                               // skip in dry-run mode
	)
	if status == 429 {
		shared.Fatalf("Too many requests for %s, aborted\n", url)
		return
	}
	if err != nil {
		return
	}
	nBytes := int64(len(res.([]byte)))
	if j.SaveArch {
		path := dirPath + "/" + GroupsioMBoxFile
		err = ioutil.WriteFile(path, res.([]byte), 0644)
		if err != nil {
			return
		}
		shared.Printf("written %s (%d bytes)\n", path, nBytes)
	} else {
		shared.Printf("read %d bytes\n", nBytes)
	}
	bytesReader := bytes.NewReader(res.([]byte))
	var zipReader *zip.Reader
	zipReader, err = zip.NewReader(bytesReader, nBytes)
	if err != nil {
		return
	}
	var messages [][]byte
	msgSep := shared.MBoxMsgSeparator[GroupsIO]
	for _, file := range zipReader.File {
		var rc io.ReadCloser
		rc, err = file.Open()
		if err != nil {
			return
		}
		var data []byte
		data, err = ioutil.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			return
		}
		shared.Printf("%s uncomressed %d bytes\n", file.Name, len(data))
		ary := bytes.Split(data, msgSep)
		shared.Printf("%s # of messages: %d\n", file.Name, len(ary))
		messages = append(messages, ary...)
	}
	shared.Printf("number of messages to parse: %d\n", len(messages))
	// Process messages (possibly in threads)
	var (
		ch         chan error
		allDocs    []interface{}
		allMsgs    []interface{}
		allMsgsMtx *sync.Mutex
		escha      []chan error
		eschaMtx   *sync.Mutex
		statMtx    *sync.Mutex
	)
	if thrN > 1 {
		ch = make(chan error)
		allMsgsMtx = &sync.Mutex{}
		eschaMtx = &sync.Mutex{}
	}
	nThreads := 0
	empty := 0
	warns := 0
	invalid := 0
	filtered := 0
	if thrN > 1 {
		statMtx = &sync.Mutex{}
	}
	stat := func(emp, warn, valid, oor bool) {
		if thrN > 1 {
			statMtx.Lock()
		}
		if emp {
			empty++
		}
		if warn {
			warns++
		}
		if !valid {
			invalid++
		}
		if oor {
			filtered++
		}
		if thrN > 1 {
			statMtx.Unlock()
		}
	}
	processMsg := func(c chan error, msg []byte) (wch chan error, e error) {
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		nBytes := len(msg)
		if nBytes < len(msgSep) {
			stat(true, false, false, false)
			return
		}
		if !bytes.HasPrefix(msg, msgSep[1:]) {
			msg = append(msgSep[1:], msg...)
		}
		var (
			valid   bool
			warn    bool
			message map[string]interface{}
		)
		message, valid, warn = shared.ParseMBoxMsg(ctx, j.GroupName, msg, GroupsIO)
		stat(false, warn, valid, false)
		if !valid {
			return
		}
		updatedOn := j.ItemUpdatedOn(message)
		if ctx.DateFrom != nil && updatedOn.Before(from) {
			stat(false, false, false, true)
			return
		}
		esItem := j.AddMetadata(ctx, message)
		if ctx.Project != "" {
			message["project"] = ctx.Project
		}
		esItem["data"] = message
		// Real data processing here
		if allMsgsMtx != nil {
			allMsgsMtx.Lock()
		}
		allMsgs = append(allMsgs, esItem)
		nMsgs := len(allMsgs)
		if nMsgs >= ctx.PackSize {
			sendToQueue := func(c chan error) (ee error) {
				defer func() {
					if c != nil {
						c <- ee
					}
				}()
				if ctx.Debug > 0 {
					shared.Printf("sending %d items to queue\n", len(allMsgs))
				}
				ee = j.GroupsioEnrichItems(ctx, thrN, allMsgs, &allDocs, false)
				// ee = SendToQueue(ctx, j, true, UUID, allMsgs)
				if ee != nil {
					shared.Printf("error %v sending %d messages to queue\n", ee, len(allMsgs))
				}
				allMsgs = []interface{}{}
				if allMsgsMtx != nil {
					allMsgsMtx.Unlock()
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
			if allMsgsMtx != nil {
				allMsgsMtx.Unlock()
			}
		}
		return
	}
	if thrN > 1 {
		for _, message := range messages {
			go func(msg []byte) {
				var (
					e    error
					esch chan error
				)
				esch, e = processMsg(ch, msg)
				if e != nil {
					shared.Printf("process message error: %v\n", e)
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
			}(message)
			nThreads++
			if nThreads == thrN {
				err = <-ch
				if err != nil {
					return
				}
				nThreads--
			}
		}
		if ctx.Debug > 0 {
			shared.Printf("joining %d threads\n", nThreads)
		}
		for nThreads > 0 {
			err = <-ch
			nThreads--
			if err != nil {
				return
			}
		}
	} else {
		for _, message := range messages {
			_, err = processMsg(nil, message)
			if err != nil {
				return
			}
		}
	}
	if ctx.Debug > 0 {
		shared.Printf("%d wait channels\n", len(escha))
	}
	for _, esch := range escha {
		err = <-esch
		if err != nil {
			return
		}
	}
	nMsgs := len(allMsgs)
	if ctx.Debug > 0 {
		shared.Printf("%d remaining messages to send to ES\n", nMsgs)
	}
	err = j.GroupsioEnrichItems(ctx, thrN, allMsgs, &allDocs, true)
	//err = SendToQueue(ctx, j, true, UUID, allMsgs)
	if err != nil {
		shared.Printf("Error %v sending %d messages to ES\n", err, len(allMsgs))
	}
	if empty > 0 {
		shared.Printf("%d empty messages\n", empty)
	}
	if warns > 0 {
		shared.Printf("%d parse message warnings\n", warns)
	}
	if invalid > 0 {
		shared.Printf("%d invalid messages\n", invalid)
	}
	if filtered > 0 {
		shared.Printf("%d filtered messages (updated before %v)\n", invalid, from)
	}
	// NOTE: Non-generic ends here
	gMaxCreatedAtMtx.Lock()
	defer gMaxCreatedAtMtx.Unlock()
	shared.SetLastUpdate(ctx, j.GroupName, gMaxCreatedAt)
	return
}

// GetItemIdentitiesEx return list of item's identities, each one is [3]string
// we use string and not *string which allows nil to allow usage as a map key
// This one (Ex) also returns information about identity's origins (from, to, or both)
func (j *DSGroupsio) GetItemIdentitiesEx(ctx *shared.Ctx, doc interface{}) (identities map[[3]string]map[string]struct{}, nRecipients int) {
	init := false
	props := []string{"From", "To"}
	for _, prop := range props {
		lProp := strings.ToLower(prop)
		ifroms, ok := shared.Dig(doc, []string{"data", prop}, false, true)
		if !ok {
			ifroms, ok = shared.Dig(doc, []string{"data", lProp}, false, true)
			if !ok {
				if ctx.Debug > 1 || lProp == "from" {
					shared.Printf("cannot get identities: cannot dig %s/%s in %v\n", prop, lProp, doc)
				}
				continue
			}
		}
		// Property can be an array
		froms, ok := ifroms.([]interface{})
		if !ok {
			// Or can be a string
			sfroms, ok := ifroms.(string)
			if !ok {
				shared.Printf("cannot get identities: cannot read string or interface array from %v\n", ifroms)
				continue
			}
			froms = []interface{}{sfroms}
		}
		for _, ifrom := range froms {
			from, ok := ifrom.(string)
			if !ok {
				shared.Printf("cannot get identities: cannot read string from %v\n", ifrom)
				continue
			}
			emails, ok := shared.ParseAddresses(ctx, from, GroupsioMaxRecipients)
			if !ok {
				if ctx.Debug > 1 {
					shared.Printf("cannot get identities: cannot read email address(es) from %s\n", from)
				}
				continue
			}
			for _, obj := range emails {
				if !init {
					identities = make(map[[3]string]map[string]struct{})
					init = true
				}
				identity := [3]string{obj.Name, "", obj.Address}
				_, ok := identities[identity]
				if !ok {
					identities[identity] = make(map[string]struct{})
				}
				identities[identity][lProp] = struct{}{}
			}
			if lProp == "to" {
				nRecipients = len(emails)
			}
		}
	}
	return
}

// EnrichItem - return rich item from raw item for a given author type/role
func (j *DSGroupsio) EnrichItem(ctx *shared.Ctx, item map[string]interface{}, role string, roleData interface{}) (rich map[string]interface{}, err error) {
	/*
		shared.Printf("raw: %s\n", shared.InterfaceToStringTrunc(item, shared.MaxPayloadPrintfLen, true))
		shared.Printf("role=(%s,%+v) item=%s\n", role, roleData, shared.DumpKeys(item))
		jsonBytes, err := jsoniter.Marshal(item)
		if err != nil {
			shared.Printf("Error: %+v\n", err)
			return
		}
		shared.Printf("%s\n", string(jsonBytes))
	*/
	rich = make(map[string]interface{})
	msg, ok := item["data"].(map[string]interface{})
	if !ok {
		err = fmt.Errorf("missing data field in item %+v", shared.DumpKeys(item))
		return
	}
	msgDate, ok := shared.Dig(msg, []string{shared.MessageDateField[GroupsIO]}, false, true)
	// original raw format support
	if !ok {
		msgDate, ok = shared.Dig(msg, []string{"Date"}, false, true)
		if !ok {
			shared.Fatalf("cannot find date/Date field in %+v\n", shared.DumpKeys(msg))
			return
		}
	}
	var (
		msgTz       float64
		msgDateInTz time.Time
	)
	iMsgDateInTz, ok1 := shared.Dig(msg, []string{"date_in_tz"}, false, true)
	if ok1 {
		msgDateInTz, ok1 = iMsgDateInTz.(time.Time)
	}
	iMsgTz, ok2 := shared.Dig(msg, []string{"date_tz"}, false, true)
	if ok2 {
		msgTz, ok2 = iMsgTz.(float64)
	}
	if !ok1 || !ok2 {
		sdt := fmt.Sprintf("%v", msgDate)
		_, msgDateInTzN, msgTzN, ok := shared.ParseDateWithTz(sdt)
		if ok {
			if !ok1 {
				msgDateInTz = msgDateInTzN
			}
			if !ok2 {
				msgTz = msgTzN
			}
		}
		if !ok && ctx.Debug > 0 {
			shared.Printf("unable to determine tz for %v/%v/%v\n", msgDate, iMsgDateInTz, iMsgTz)
		}
	}
	// copy RawFields
	if role == "author" {
		for _, field := range shared.RawFields {
			v, _ := item[field]
			rich[field] = v
		}
		getStr := func(i interface{}) (o string, ok bool) {
			o, ok = i.(string)
			if ok {
				//Printf("getStr(%v) -> string:%s\n", i, o)
				return
			}
			var a []interface{}
			a, ok = i.([]interface{})
			if !ok {
				//Printf("getStr(%v) -> neither string nor []interface{}: %T\n", i, i)
				return
			}
			if len(a) == 0 {
				ok = false
				//Printf("getStr(%v) -> empty array\n", i)
				return
			}
			la := len(a)
			o, ok = a[la-1].(string)
			//Printf("getStr(%v) -> string[0]:%s\n", i, o)
			return
		}
		getStringValue := func(it map[string]interface{}, key string) (val string, ok bool) {
			var i interface{}
			i, ok = shared.Dig(it, []string{key}, false, true)
			if ok {
				val, ok = getStr(i)
				if ok {
					//Printf("getStringValue(%v) -> string:%s\n", key, val)
					return
				}
				//Printf("getStringValue(%v) - was not able to get string from %v\n", key, i)
			}
			lKey := strings.ToLower(key)
			//Printf("getStringValue(%v) -> key not found, trying %s\n", key, lKey)
			for k := range it {
				if k == key {
					continue
				}
				lK := strings.ToLower(k)
				if lK == lKey {
					//Printf("getStringValue(%v) -> %s matches\n", key, k)
					i, ok = shared.Dig(it, []string{k}, false, true)
					if ok {
						val, ok = getStr(i)
						if ok {
							//Printf("getStringValue(%v) -> %s string:%s\n", key, k, val)
							return
						}
						//Printf("getStringValue(%v) - %s was not able to get string from %v\n", key, k, i)
					}
				}
			}
			//Printf("getStringValue(%v) -> key not found\n", key)
			return
		}
		getIValue := func(it map[string]interface{}, key string) (i interface{}, ok bool) {
			i, ok = shared.Dig(it, []string{key}, false, true)
			if ok {
				//Printf("getIValue(%v) -> %T:%v\n", key, i, i)
				return
			}
			lKey := strings.ToLower(key)
			//Printf("getIValue(%v) -> key not found, trying %s\n", key, lKey)
			for k := range it {
				if k == key {
					continue
				}
				lK := strings.ToLower(k)
				if lK == lKey {
					//Printf("getIValue(%v) -> %s matches\n", key, k)
					i, ok = shared.Dig(it, []string{k}, false, true)
					if ok {
						//Printf("getIValue(%v) -> %s %T:%v\n", key, k, i, i)
						return
					}
				}
			}
			//Printf("getIValue(%v) -> key not found\n", key)
			return
		}
		rich["Message-ID"], ok = shared.Dig(msg, []string{shared.MessageIDField[GroupsIO]}, false, true)
		// original raw format support
		if !ok {
			rich["Message-ID"], ok = shared.Dig(msg, []string{"Message-ID"}, false, true)
			if !ok {
				shared.Fatalf("cannot find message-id/Message-ID field in %v\n", shared.DumpKeys(msg))
				return
			}
		}
		rich["Date"] = msgDate
		rich["Date_in_tz"] = msgDateInTz
		rich["tz"] = msgTz
		subj, _ := getStringValue(msg, "Subject")
		rich["Subject_analyzed"] = subj
		if len(subj) > shared.MaxMessageBodyLength[GroupsIO] {
			subj = subj[:shared.MaxMessageBodyLength[GroupsIO]]
		}
		rich["Subject"] = subj
		rich["email_date"], _ = getIValue(item, "metadata__updated_on")
		parentMessageID, okParent := getStringValue(item, "In-Reply-To")
		if okParent {
			rich["parent_message_id"] = parentMessageID
		}
		rich["list"], _ = getStringValue(item, "origin")
		lks := make(map[string]struct{})
		for k := range msg {
			lks[strings.ToLower(k)] = struct{}{}
		}
		_, ok = lks["in-reply-to"]
		rich["root"] = !ok
		var (
			plain interface{}
			text  string
			found bool
		)
		plain, ok = shared.Dig(msg, []string{"data", "text", "plain"}, false, true)
		if ok {
			a, ok := plain.([]interface{})
			if ok {
				if len(a) > 0 {
					body, ok := a[0].(map[string]interface{})
					if ok {
						data, ok := body["data"]
						if ok {
							text, found = data.(string)
						}
					}
				}
			}
		} else {
			// original raw format support
			plain, ok = shared.Dig(msg, []string{"body", "plain"}, false, true)
			if ok {
				text, found = plain.(string)
			}
		}
		if found {
			rich["size"] = len(text)
			ary := strings.Split(text, "\n")
			if len(ary) > GroupsioMaxRichMessageLines {
				ary = ary[:GroupsioMaxRichMessageLines]
			}
			text = strings.Join(ary, "\n")
			if len(text) > shared.MaxMessageBodyLength[GroupsIO] {
				text = text[:shared.MaxMessageBodyLength[GroupsIO]]
			}
			rich["body_extract"] = text
		} else {
			rich["size"] = nil
			rich["body_extract"] = ""
		}
		rich["mbox_parse_warning"], _ = shared.Dig(msg, []string{"MBox-Warn"}, false, true)
		rich["mbox_bytes_length"], _ = shared.Dig(msg, []string{"MBox-Bytes-Length"}, false, true)
		rich["mbox_n_lines"], _ = shared.Dig(msg, []string{"MBox-N-Lines"}, false, true)
		rich["mbox_n_bodies"], _ = shared.Dig(msg, []string{"MBox-N-Bodies"}, false, true)
		rich["mbox_from"], _ = shared.Dig(msg, []string{"MBox-From"}, false, true)
		rich["mbox_date"] = nil
		rich["mbox_date_str"] = ""
		dtStr, ok := shared.Dig(msg, []string{"MBox-Date"}, false, true)
		if ok {
			sdt, ok := dtStr.(string)
			if ok {
				rich["mbox_date_str"] = sdt
				dt, dttz, tz, valid := shared.ParseDateWithTz(sdt)
				if valid {
					rich["mbox_date"] = dt
					rich["mbox_date_tz"] = tz
					rich["mbox_date_in_tz"] = dttz
				}
			}
		}
	}
	var dt time.Time
	dt, err = shared.TimeParseInterfaceString(msgDate)
	if err != nil {
		switch vdt := msgDate.(type) {
		case string:
			dt, _, _, ok = shared.ParseDateWithTz(vdt)
			if !ok {
				err = fmt.Errorf("cannot parse date %s", vdt)
				return
			}
		case time.Time:
			dt = vdt
		default:
			err = fmt.Errorf("cannot parse date %T %v", vdt, vdt)
			return
		}
		err = nil
	}
	rich["date_parsed"] = dt
	rich[role] = roleData
	// From shared
	rich["metadata__enriched_on"] = time.Now()
	// rich[ProjectSlug] = ctx.ProjectSlug
	// rich["groups"] = ctx.Groups
	return
}

// GroupsioEnrichItems - iterate items and enrich them
// items is a current pack of input items
// docs is a pointer to where extracted identities will be stored
func (j *DSGroupsio) GroupsioEnrichItems(ctx *shared.Ctx, thrN int, items []interface{}, docs *[]interface{}, final bool) (err error) {
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
			gMaxCreatedAtMtx.Lock()
			defer gMaxCreatedAtMtx.Unlock()
			shared.SetLastUpdate(ctx, j.GroupName, gMaxCreatedAt)
		}
	}
	if final {
		defer func() {
			outputDocs()
		}()
	}
	// NOTE: non-generic code starts
	if ctx.Debug > 0 {
		shared.Printf("groupsio enrich items %d/%d func\n", len(items), len(*docs))
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
		item := items[idx]
		if thrN > 1 {
			mtx.RUnlock()
		}
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		// NOTE: never refer to _source - we no longer use ES
		doc, ok := item.(map[string]interface{})
		if !ok {
			e = fmt.Errorf("Failed to parse document %+v", doc)
			return
		}
		identities, nRecipients := j.GetItemIdentitiesEx(ctx, doc)
		if identities == nil || len(identities) == 0 {
			if ctx.Debug > 1 {
				shared.Printf("no identities to enrich in %v\n", doc)
			}
			return
		}
		counts := make(map[string]int)
		getAuthorPrefix := func(origin string) (author string) {
			origin = strings.ToLower(origin)
			cnt, _ := counts[origin]
			cnt++
			counts[origin] = cnt
			author = "author"
			if origin != "from" {
				author = "recipient"
			}
			if cnt > 1 {
				author += strconv.Itoa(cnt)
			}
			return
		}
		var rich map[string]interface{}
		authorFound := false
		for identity, origins := range identities {
			for origin := range origins {
				var richPart map[string]interface{}
				auth := getAuthorPrefix(origin)
				if rich == nil {
					rich, e = j.EnrichItem(ctx, doc, auth, identity)
				} else {
					richPart, e = j.EnrichItem(ctx, doc, auth, identity)
				}
				if e != nil {
					return
				}
				if auth == "author" {
					authorFound = true
				}
				if richPart != nil {
					for k, v := range richPart {
						if strings.HasPrefix(k, "recipient") {
							recipient, _ := v.([3]string)
							iRecipients, ok := rich["recipients"]
							if ok {
								recipients, _ := iRecipients.(map[[3]string]struct{})
								recipients[recipient] = struct{}{}
								rich["recipients"] = recipients
								// shared.Printf("more recipients: %+v\n", recipients)
							} else {
								rich["recipients"] = map[[3]string]struct{}{recipient: {}}
								// shared.Printf("first recipient: %+v\n", rich["recipients"])
							}
							continue
						}
						_, ok := rich[k]
						if !ok {
							rich[k] = v
						}
					}
				}
			}
		}
		if !authorFound {
			if ctx.Debug > 1 {
				shared.Printf("no author found in\n%v\n%v\n", identities, item)
			} else if ctx.Debug > 0 {
				shared.Printf("skipping email due to missing usable from email %v\n", identities)
			}
			return
		}
		rich["n_recipients"] = nRecipients
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

// GetModelData - return data in swagger format
func (j *DSGroupsio) GetModelData(ctx *shared.Ctx, docs []interface{}) (data *models.Data) {
	url := GroupsioURLRoot + j.GroupName
	data = &models.Data{
		DataSource: GroupsioDataSource,
		MetaData:   gGroupsioMetaData,
		Endpoint:   url,
	}
	source := data.DataSource.Slug
	for _, iDoc := range docs {
		var (
			parentInternalID *string
			parentID         *string
		)
		doc, _ := iDoc.(map[string]interface{})
		// shared.Printf("rich %+v\n", doc)
		docUUID, _ := doc["uuid"].(string)
		messageID, _ := doc["Message-ID"].(string)
		subject, _ := doc["Subject_analyzed"].(string)
		body, _ := doc["body_extract"].(string)
		createdOn, _ := doc["date_parsed"].(time.Time)
		gMaxCreatedAtMtx.Lock()
		if createdOn.After(gMaxCreatedAt) {
			gMaxCreatedAt = createdOn
		}
		gMaxCreatedAtMtx.Unlock()
		createdOnInTz, _ := doc["Date_in_tz"].(time.Time)
		createdTz, _ := doc["tz"].(float64)
		sParent, okParent := doc["parent_message_id"].(string)
		if okParent {
			parentInternalID = &sParent
			sParentID := shared.UUIDNonEmpty(ctx, url, sParent)
			parentID = &sParentID
		}
		sender, _ := doc["author"].([3]string)
		recipients := []*models.Identity{}
		iRecipients, ok := doc["recipients"]
		if ok {
			recs, _ := iRecipients.(map[[3]string]struct{})
			for recipient := range recs {
				name := recipient[0]
				username := recipient[1]
				email := recipient[2]
				name, username = shared.PostprocessNameUsername(name, username, email)
				userUUID := shared.UUIDAffs(ctx, source, email, name, username)
				recipients = append(recipients, &models.Identity{
					ID:           userUUID,
					DataSourceID: source,
					Name:         name,
					Username:     username,
					Email:        email,
				})
			}
		}
		name := sender[0]
		username := sender[1]
		email := sender[2]
		name, username = shared.PostprocessNameUsername(name, username, email)
		userUUID := shared.UUIDAffs(ctx, source, email, name, username)
		// Event
		event := &models.Event{
			Message: &models.Message{
				DataSourceID:  source,
				MessageID:     docUUID,
				InternalID:    messageID,
				Subject:       subject,
				Body:          body,
				CreatedAt:     strfmt.DateTime(createdOn),
				CreatedAtInTZ: strfmt.DateTime(createdOnInTz),
				CreatedTZ:     createdTz,
				Sender: &models.Identity{
					ID:           userUUID,
					DataSourceID: source,
					Name:         name,
					Username:     username,
					Email:        email,
				},
				Recipients:       recipients,
				ParentInternalID: parentInternalID,
				ParentID:         parentID,
				MailingList: &models.MailingList{
					InternalID: fmt.Sprintf("%d", j.GroupID),
					URL:        url,
				},
			},
		}
		data.Events = append(data.Events, event)
	}
	return
}

func main() {
	var (
		ctx      shared.Ctx
		groupsio DSGroupsio
	)
	err := groupsio.Init(&ctx)
	if err != nil {
		shared.Printf("Error: %+v\n", err)
		return
	}
	err = groupsio.Sync(&ctx)
	if err != nil {
		shared.Printf("Error: %+v\n", err)
		return
	}
}
