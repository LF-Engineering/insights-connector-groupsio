package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	neturl "net/url"

	"github.com/LF-Engineering/insights-datasource-groupsio/gen/models"
	shared "github.com/LF-Engineering/insights-datasource-shared"
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
	// GroupsioMaxMessageBodyLength - trucacte message bodies longer than this (per each multi-body email part)
	GroupsioMaxMessageBodyLength = 0x4000
	// GroupsioMaxRichMessageLines - maximum number of message text/plain lines copied to rich index
	GroupsioMaxRichMessageLines = 100
	// GroupsioMaxRecipients - maximum number of emails parsed from To:
	GroupsioMaxRecipients = 100
)

var (
	gMaxCreatedAt    time.Time
	gMaxCreatedAtMtx = &sync.Mutex{}
	// GroupsioDataSource - constant
	GroupsioDataSource = &models.DataSource{Name: "Groups.io", Slug: "groupsio"}
	gGroupsioMetaData  = &models.MetaData{BackendName: "groupsio", BackendVersion: GroupsioBackendVersion}
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
	fmt.Printf("threads: %d\n", thrN)
	// FIXME
	gMaxCreatedAtMtx.Lock()
	defer gMaxCreatedAtMtx.Unlock()
	shared.SetLastUpdate(ctx, j.GroupName, gMaxCreatedAt)
	return
}

/*
// GetModelData - return data in swagger format
func (j *DSGroupsio) GetModelData(ctx *shared.Ctx, docs []interface{}) (data *models.Data) {
	data = &models.Data{
		DataSource: ConfluenceDataSource,
		MetaData:   gConfluenceMetaData,
    Endpoint:   j.URL,
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
		gMaxCreatedAtMtx.Lock()
		if updatedOn.After(gMaxCreatedAt) {
			gMaxCreatedAt = updatedOn
		}
		gMaxCreatedAtMtx.Unlock()
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
					UpdatedAt:       strfmt.DateTime(updatedOn),
					Title:           title,
					URL:             url,
					Space:           &space,
					DataSourceID:    source,
					DocumentType:    origType,
					DocumentVersion: fmt.Sprintf("%.0f", version),
					Ancestors:       ancestors,
				},
			},
		}
		data.Events = append(data.Events, event)
	}
	return
}
*/

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
