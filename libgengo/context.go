package libgengo

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

type ROSPackage struct {
	Name string `xml:"name"`
}

func isRosPackage(dir string) bool {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, f := range files {
		if f.Name() == "package.xml" {
			return true
		}
	}
	return false
}

func findPackages(pkgType string, rosPkgPaths []string) (map[string]string, error) {
	pkgs := make(map[string]string)

	recurseDirForMsgs := func(path string, info os.FileInfo, err error) error {
		// Check whether we could open the path ok.
		if err != nil {
			// Just skip this item and try the next one.
			return nil
		}

		// Check whether this path is a directory.
		if !info.IsDir() {
			// It's not a directory, so we skip it; we only care about directories.
			return nil
		}

		// Check whether this directory is a ROS package.
		if isRosPackage(path) {
			// It's a ROS package.
			packageDefinitionFile := filepath.Join(path, "package.xml")
			if packageDefinitionData, err := os.ReadFile(packageDefinitionFile); err != nil {
				return err
			} else {
				packageDefinition := ROSPackage{}
				if err := xml.Unmarshal(packageDefinitionData, &packageDefinition); err != nil {
					return err
				} else {
					pkgName := packageDefinition.Name
					pkgPath := filepath.Join(path, pkgType)
					pkgPaths, err := filepath.Glob(pkgPath + fmt.Sprintf("/*.%s", pkgType))
					if err != nil {
						return nil
					}
					for _, p := range pkgPaths {
						basename := filepath.Base(p)
						rootname := basename[:len(basename)-(len(pkgType)+1)] // This is chopping off the file extension.  Horribly.
						fullname := pkgName + "/" + rootname
						pkgs[fullname] = p
					}

					// No point checking INSIDE this one, since it's already a package.
					return filepath.SkipDir
				}
			}
		}

		// Else just keep walking.
		return nil
	}

	// Iterate over the list of paths to search.
	for _, p := range rosPkgPaths {
		err := filepath.Walk(p, recurseDirForMsgs)
		if err != nil {
			// If someone complains, then we just skip searching the reset of this path.
			continue
		}
	}

	return pkgs, nil
}

func FindAllMessages(rosPkgPaths []string) (map[string]string, error) {
	return findPackages("msg", rosPkgPaths)
}

func FindAllServices(rosPkgPaths []string) (map[string]string, error) {
	return findPackages("srv", rosPkgPaths)
}

func FindAllActions(rosPkgPaths []string) (map[string]string, error) {
	return findPackages("action", rosPkgPaths)
}

type PkgContext struct {
	msgPathMap      map[string]string
	msgRegistry     map[string]*MsgSpec
	msgRegistryLock sync.RWMutex

	srvPathMap      map[string]string
	srvRegistry     map[string]*SrvSpec
	srvRegistryLock sync.RWMutex

	actionPathMap   map[string]string
	actRegistry     map[string]*ActionSpec
	actRegistryLock sync.RWMutex
}

func NewPkgContext(rosPkgPaths []string) (*PkgContext, error) {
	ctx := new(PkgContext)
	msgs, err := FindAllMessages(rosPkgPaths)
	if err != nil {
		return nil, err
	}
	ctx.msgPathMap = msgs

	srvs, err := FindAllServices(rosPkgPaths)
	if err != nil {
		return nil, err
	}
	ctx.srvPathMap = srvs

	acts, err := FindAllActions(rosPkgPaths)
	if err != nil {
		return nil, err
	}
	ctx.actionPathMap = acts

	ctx.msgRegistry = make(map[string]*MsgSpec)
	ctx.srvRegistry = make(map[string]*SrvSpec)
	ctx.actRegistry = make(map[string]*ActionSpec)
	return ctx, nil
}

func (ctx *PkgContext) RegisterMsg(fullname string, spec *MsgSpec) {
	ctx.msgRegistryLock.Lock()
	ctx.msgRegistry[fullname] = spec
	ctx.msgRegistryLock.Unlock()
}

func (ctx *PkgContext) RegisterSrv(fullname string, spec *SrvSpec) {
	ctx.srvRegistryLock.Lock()
	ctx.srvRegistry[fullname] = spec
	ctx.srvRegistryLock.Unlock()
}

func (ctx *PkgContext) RegisterAction(fullname string, spec *ActionSpec) {
	ctx.actRegistryLock.Lock()
	ctx.actRegistry[fullname] = spec
	ctx.actRegistryLock.Unlock()
}

func (ctx *PkgContext) LoadMsgFromString(text string, fullname string) (*MsgSpec, error) {
	packageName, shortName, e := packageResourceName(fullname)
	if e != nil {
		return nil, e
	}

	fields := make([]Field, 0)
	constants := make([]Constant, 0)
	for lineno, origLine := range strings.Split(text, "\n") {
		cleanLine := stripComment(origLine)
		if len(cleanLine) == 0 {
			// Skip empty line
			continue
		} else if strings.Contains(cleanLine, ConstChar) {
			constant, e := loadConstantLine(origLine)
			if e != nil {
				return nil, NewSyntaxError(fullname, lineno, e.Error())
			}
			constants = append(constants, *constant)
		} else {
			field, e := loadFieldLine(origLine, packageName)
			if e != nil {
				return nil, NewSyntaxError(fullname, lineno, e.Error())
			}
			fields = append(fields, *field)
		}
	}
	spec, _ := NewMsgSpec(fields, constants, text, fullname, OptionPackageName(packageName), OptionShortName(shortName))
	var err error
	md5sum, err := ctx.ComputeMsgMD5(spec)
	if err != nil {
		return nil, err
	}
	spec.MD5Sum = md5sum
	ctx.RegisterMsg(fullname, spec)
	return spec, nil
}

func (ctx *PkgContext) LoadMsgFromFile(filePath string, fullname string) (*MsgSpec, error) {
	bytes, e := ioutil.ReadFile(filePath)
	if e != nil {
		return nil, e
	}
	text := string(bytes)
	return ctx.LoadMsgFromString(text, fullname)
}

func (ctx *PkgContext) LoadMsg(fullname string) (*MsgSpec, error) {
	ctx.msgRegistryLock.RLock()
	if spec, ok := ctx.msgRegistry[fullname]; ok {
		ctx.msgRegistryLock.RUnlock()
		return spec, nil
	} else {
		ctx.msgRegistryLock.RUnlock()
		if path, ok := ctx.msgPathMap[fullname]; ok {
			spec, err := ctx.LoadMsgFromFile(path, fullname)
			if err != nil {
				return nil, err
			} else {
				ctx.msgRegistryLock.Lock()
				ctx.msgRegistry[fullname] = spec
				ctx.msgRegistryLock.Unlock()
				return spec, nil
			}
		} else {
			return nil, fmt.Errorf("message definition of `%s` is not found", fullname)
		}
	}
}

func (ctx *PkgContext) LoadSrvFromString(text string, fullname string) (*SrvSpec, error) {
	packageName, shortName, err := packageResourceName(fullname)
	if err != nil {
		return nil, err
	}

	rex := regexp.MustCompile("[-]{3,}")
	components := rex.Split(text, -1)
	//components := strings.Split(text, "---")
	if len(components) != 2 {
		return nil, fmt.Errorf("syntax error: missing delimiter '---'")
	}

	reqText := components[0]
	resText := components[1]

	reqSpec, err := ctx.LoadMsgFromString(reqText, fullname+"Request")
	if err != nil {
		return nil, err
	}
	resSpec, err := ctx.LoadMsgFromString(resText, fullname+"Response")
	if err != nil {
		return nil, err
	}

	spec := &SrvSpec{
		packageName, shortName, fullname, text, "", reqSpec, resSpec,
	}
	md5sum, err := ctx.ComputeSrvMD5(spec)
	if err != nil {
		return nil, err
	}
	spec.MD5Sum = md5sum
	ctx.RegisterSrv(fullname, spec)
	return spec, nil
}

func (ctx *PkgContext) LoadSrvFromFile(filePath string, fullname string) (*SrvSpec, error) {
	bytes, e := ioutil.ReadFile(filePath)
	if e != nil {
		return nil, e
	}
	text := string(bytes)
	return ctx.LoadSrvFromString(text, fullname)
}

func (ctx *PkgContext) LoadSrv(fullname string) (*SrvSpec, error) {
	if path, ok := ctx.srvPathMap[fullname]; ok {
		spec, err := ctx.LoadSrvFromFile(path, fullname)
		if err != nil {
			return nil, err
		} else {
			return spec, nil
		}
	} else {
		return nil, fmt.Errorf("service definition of `%s` is not found", fullname)
	}
}

func (ctx *PkgContext) LoadActionFromString(text string, fullname string) (*ActionSpec, error) {
	packageName, shortName, err := packageResourceName(fullname)
	if err != nil {
		return nil, err
	}

	rex := regexp.MustCompile("[-]{3,}")
	components := rex.Split(text, -1)
	//components := strings.Split(text, "---")
	if len(components) != 3 {
		return nil, fmt.Errorf("syntax error: missing delimiter(s) '---'")
	}

	goalText := components[0]
	resultText := components[1]
	feedbackText := components[2]
	goalSpec, err := ctx.LoadMsgFromString(goalText, fullname+"Goal")
	if err != nil {
		return nil, err
	}
	actionGoalText := "Header header\nactionlib_msgs/GoalID goal_id\n" + fullname + "Goal goal\n"
	actionGoalSpec, err := ctx.LoadMsgFromString(actionGoalText, fullname+"ActionGoal")
	if err != nil {
		return nil, err
	}
	feedbackSpec, err := ctx.LoadMsgFromString(feedbackText, fullname+"Feedback")
	if err != nil {
		return nil, err
	}
	actionFeedbackText := "Header header\nactionlib_msgs/GoalStatus status\n" + fullname + "Feedback feedback"
	actionFeedbackSpec, err := ctx.LoadMsgFromString(actionFeedbackText, fullname+"ActionFeedback")
	if err != nil {
		return nil, err
	}
	resultSpec, err := ctx.LoadMsgFromString(resultText, fullname+"Result")
	if err != nil {
		return nil, err
	}
	actionResultText := "Header header\nactionlib_msgs/GoalStatus status\n" + fullname + "Result result"
	actionResultSpec, err := ctx.LoadMsgFromString(actionResultText, fullname+"ActionResult")
	if err != nil {
		return nil, err
	}

	spec := &ActionSpec{
		Package:        packageName,
		ShortName:      shortName,
		FullName:       fullname,
		Text:           text,
		Goal:           goalSpec,
		Feedback:       feedbackSpec,
		Result:         resultSpec,
		ActionGoal:     actionGoalSpec,
		ActionFeedback: actionFeedbackSpec,
		ActionResult:   actionResultSpec,
	}

	md5sum, err := ctx.ComputeActionMD5(spec)
	if err != nil {
		return nil, err
	}
	spec.MD5Sum = md5sum
	ctx.RegisterAction(fullname, spec)
	return spec, nil
}

func (ctx *PkgContext) LoadActionFromFile(filePath string, fullname string) (*ActionSpec, error) {
	bytes, e := ioutil.ReadFile(filePath)
	if e != nil {
		return nil, e
	}
	text := string(bytes)
	return ctx.LoadActionFromString(text, fullname)
}

func (ctx *PkgContext) LoadAction(fullname string) (*ActionSpec, error) {
	ctx.actRegistryLock.RLock()
	if spec, ok := ctx.actRegistry[fullname]; ok {
		ctx.actRegistryLock.RUnlock()
		return spec, nil
	} else {
		ctx.actRegistryLock.RUnlock()
		if path, ok := ctx.actionPathMap[fullname]; ok {
			spec, err := ctx.LoadActionFromFile(path, fullname)
			if err != nil {
				return nil, err
			} else {
				ctx.actRegistryLock.Lock()
				ctx.actRegistry[fullname] = spec
				ctx.actRegistryLock.Unlock()
				return spec, nil
			}
		} else {
			return nil, fmt.Errorf("action definition of `%s` is not found", fullname)
		}
	}
}

func (ctx *PkgContext) ComputeMD5Text(spec *MsgSpec) (string, error) {
	var buf bytes.Buffer
	for _, c := range spec.Constants {
		buf.WriteString(fmt.Sprintf("%s %s=%s\n", c.Type, c.Name, c.ValueText))
	}
	for _, f := range spec.Fields {
		if f.Package == "" {
			buf.WriteString(fmt.Sprintf("%s\n", f.String()))
		} else {
			subspec, err := ctx.LoadMsg(f.Package + "/" + f.Type)
			if err != nil {
				return "", nil
			}
			submd5, err := ctx.ComputeMsgMD5(subspec)
			if err != nil {
				return "", nil
			}
			buf.WriteString(fmt.Sprintf("%s %s\n", submd5, f.Name))
		}
	}
	return strings.Trim(buf.String(), "\n"), nil
}

func (ctx *PkgContext) ComputeMsgMD5(spec *MsgSpec) (string, error) {
	md5text, err := ctx.ComputeMD5Text(spec)
	if err != nil {
		return "", err
	}
	hash := md5.New()
	hash.Write([]byte(md5text))
	sum := hash.Sum(nil)
	md5sum := hex.EncodeToString(sum)
	return md5sum, nil
}

func (ctx *PkgContext) ComputeActionMD5(spec *ActionSpec) (string, error) {
	goalText, err := ctx.ComputeMD5Text(spec.ActionGoal)
	if err != nil {
		return "", err
	}
	feedbackText, err := ctx.ComputeMD5Text(spec.ActionFeedback)
	if err != nil {
		return "", err
	}
	resultText, err := ctx.ComputeMD5Text(spec.ActionResult)
	if err != nil {
		return "", err
	}
	hash := md5.New()
	hash.Write([]byte(goalText))
	hash.Write([]byte(feedbackText))
	hash.Write([]byte(resultText))
	sum := hash.Sum(nil)
	md5sum := hex.EncodeToString(sum)
	return md5sum, nil
}

func (ctx *PkgContext) ComputeSrvMD5(spec *SrvSpec) (string, error) {
	reqText, err := ctx.ComputeMD5Text(spec.Request)
	if err != nil {
		return "", err
	}
	resText, err := ctx.ComputeMD5Text(spec.Response)
	if err != nil {
		return "", err
	}
	hash := md5.New()
	hash.Write([]byte(reqText))
	hash.Write([]byte(resText))
	sum := hash.Sum(nil)
	md5sum := hex.EncodeToString(sum)
	return md5sum, nil
}

func (ctx *PkgContext) GetMsgs() map[string]*MsgSpec {
	ctx.msgRegistryLock.RLock()
	msgs := ctx.msgRegistry
	ctx.msgRegistryLock.RUnlock()
	return msgs
}

func (ctx *PkgContext) GetSrvs() map[string]*SrvSpec {
	ctx.srvRegistryLock.RLock()
	srvs := ctx.srvRegistry
	ctx.srvRegistryLock.RUnlock()
	return srvs
}

func (ctx *PkgContext) GetActions() map[string]*ActionSpec {
	ctx.actRegistryLock.RLock()
	actions := ctx.actRegistry
	ctx.actRegistryLock.RUnlock()
	return actions
}
