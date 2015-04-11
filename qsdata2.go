// qsdata2 collects information about data storage from various commands on a linux box, and outputs it in csv-formatted file
//
// This enables an enterprise csvfile-based ETL environment to monitor linux servers and desktops
//
// The program works with local disk (including HP controllers), NFS4 imports and exports and ISCSI imports and exports.
// Various heuristics are applied when combining the individual command outputs into a single matrix, in order to make disparate data appear consistent
package main

import (
	"fmt"
	"github.com/LDCS/genutil"
	"github.com/LDCS/qslinux/blkid"
	qsdf "github.com/LDCS/qslinux/df"
	"github.com/LDCS/qslinux/dmidecode"
	"github.com/LDCS/qslinux/hp"
	"github.com/LDCS/qslinux/md"
	"github.com/LDCS/qslinux/parted"
	"github.com/LDCS/qslinux/scsi"
	"github.com/LDCS/qslinux/smartctl"
	"github.com/LDCS/qslinux/tgtd"
	"github.com/LDCS/sflag"
	"strings"
)

var (
	opt = struct {
		Usage   string "wrapper for dmidecode+md+df+lsscsi+parted+tgtd functionality"
		Odir    string "output directory (default is to use stdout) |"
		Verbose bool   "verbosity | false"
	}{}
)

func main() {
	ostr := ""
	ostrVerbose := ""
	hpstr := ""
	mybox := genutil.Hostname()
	sflag.Parse(&opt)
	if opt.Verbose {
		fmt.Println("\nStarting on ", mybox, "verbose=", opt.Verbose)
	}

	dmidecode1 := dmidecode.DoListDmidecodedata(opt.Verbose)
	hpmap := hp.Nil()
	switch dmidecode1.Manufacturer_ {
	case "HP":
		hpmap = hp.DoListHpdata(opt.Verbose)
		hpstr = hpmap.SprintAll(mybox)
	default:
	}

	dfmap := qsdf.DoListDfdata(false, opt.Verbose)
	mdmap := md.DoListMddata(opt.Verbose)
	partedmap := parted.DoListParteddata(opt.Verbose)
	scsimap := scsi.DoListScsidata(opt.Verbose)
	tgtdmap := tgtd.DoListTgtddata(opt.Verbose)
	blkidmap := blkid.DoListBlkiddata(opt.Verbose)

	if true {
		ostr += fmt.Sprintf("box,%s,%s,%s,%s,%s,%s,%s,%s\n", dmidecode.Header(), qsdf.Header(), md.Header(), scsi.Header(), parted.Header(), smartctl.Header(), tgtd.Header(), blkid.Header())
		strs := genutil.SortedUniqueKeys(md.Keys_String2PtrMddata(&mdmap), scsi.Keys_String2PtrScsidata(&scsimap), parted.Keys_String2PtrParteddata(&partedmap))
		strsDf := qsdf.Keys_String2PtrDfdata(&dfmap)
		strsTgtd := tgtd.Keys_String2PtrTgtddata(&tgtdmap)
		strsBlkid := genutil.CopyStrSlice(blkid.Keys_String2PtrBlkiddata(&blkidmap), "blkid:", "")
		strsDone := map[string]bool{}
		tgtdDone := map[string]bool{}
		blkidDone := map[string]bool{}
		dfCount := map[string]int{}
		// if opt.Verbose{ ostrVerbose	+= fmt.Sprintf("keys=%s\n", strings.Join(strs, "#")) }
		// if opt.Verbose{ ostrVerbose	+= fmt.Sprintf("df=%s\n", strings.Join(strsDf, "#")) }
		// if opt.Verbose{ ostrVerbose	+= fmt.Sprintf("tgtd=%s\n", strings.Join(strsTgtd, "#")) }
		for _, kk := range append(append(append(strs, strsDf...), strsTgtd...), strsBlkid...) {
			if strings.HasPrefix(kk, "blkid:") { // This would be true after handling all keys that are more fundamental than blkid
				kk = kk[6:]
				if blkidDone[kk] {
					continue
				}
				if true {
					fmt.Println("qsdata2: unused blkid: kk=", kk)
				}
			}
			switch {
			case strsDone[kk]:
				continue // skip tgtd whose key was seen as another regular object
			case tgtdDone[kk]:
				continue // skip tgtd whose key was seen from indirection of another object
			case dfCount[kk] > 0:
				continue // skip df whose key was seen from indirection of another object
			}
			strsDone[kk] = true
			md, _ := mdmap[kk]
			scsi, _ := scsimap[kk]
			dfArr, _ := dfmap[kk]
			if len(dfArr) == 0 {
				dfArr = append(dfArr, nil)
			}
			for _, df0 := range dfArr {
				partedArr, _ := partedmap[kk]
				if (len(partedArr) == 0) && (df0 != nil) {
					partedArr, _ = partedmap[df0.Name_]
				}
				if len(partedArr) == 0 {
					partedArr = append(partedArr, nil)
				}
				for _, parted := range partedArr {
					df := df0
					blkid := (*blkid.Blkiddata)(nil)
					if (parted != nil) && parted.Skip_ {
						continue
					}
					tgtd, _ := tgtdmap[kk]
					if parted != nil {
						if _, ok := tgtdmap[parted.Path_]; ok {
							tgtdDone[parted.Path_] = true
							// if opt.Verbose{ ostrVerbose	+= fmt.Sprintf("tgtd=%s\n", parted.Path_) }
							tgtd, _ = tgtdmap[parted.Path_]
						}
					}
					if (df != nil) && (tgtd != nil) && (len(tgtd.Targetpath_) > 0) && (df.Name_ != tgtd.Targetpath_) {
						df = nil
					}
					if df == nil {
						df = qsdf.New()
					} else {
						dfCount[df.Name_] = dfCount[df.Name_] + 1
					}
					smartctl := smartctl.DoListSmartctldataOne(df, scsi, parted, dmidecode1, hpmap, opt.Verbose)
					if opt.Verbose {
						ostr += "kk=" + kk + "/dfName=" + df.Name_ + "/dfDevname=" + df.DevName_
						if parted == nil {
							ostr += "/parted=nil"
						} else {
							ostr += "/parted=" + parted.Path_
						}
						if tgtd == nil {
							ostr += "/tgtpath=nil"
						} else {
							ostr += "/tgtpath=" + tgtd.Targetpath_
						}
						ostr += ":"
					}
					switch {
					case (blkid == nil) && (parted != nil) && (blkidmap[parted.Path_] != nil):
						blkid = blkidmap[parted.Path_] // see if blkid info is available for the partition
					case (blkid == nil) && (scsi != nil) && (blkidmap[scsi.Device_] != nil):
						blkid = blkidmap[scsi.Device_] // see if blkid info is available for the device
					case (blkid == nil) && (df != nil) && (blkidmap[df.Name_] != nil):
						blkid = blkidmap[df.Name_] // see if blkid info is available for the disk
					case (blkid == nil) && (md != nil) && (blkidmap[md.Name_] != nil):
						blkid = blkidmap[md.Name_] // see if blkid info is available for the disk
					}
					skiprow := false
					switch {
					case df == nil: // cannot happen
					case len(df.Name_) <= 0: // implies df.DevName_ is also empty, so let us pick a name from elsewhere
						if scsi != nil {
							// clear df
							df1 := qsdf.New()
							df1.Name_ = scsi.Device_
							df = df1
						}
					case df.DevName_ == df.Name_:
					case parted == nil:
					case len(parted.Path_) <= 0:
					case parted.DevPath_ == parted.Path_:
					case parted.Path_ != df.Name_:
						if strings.HasPrefix(parted.Path_, df.DevName_) {
							// clear df
							df1 := qsdf.New()
							df1.Name_ = df.DevName_
							df = df1
						} else {
							skiprow = true
						}
					}
					if skiprow {
						if opt.Verbose {
							ostr += genutil.StrTernary(skiprow, "!", "")
						} else {
							continue
						}
					}
					if blkid != nil {
						blkidDone[blkid.Devname_] = true
					}
					ostr += fmt.Sprintf("%s%s,%s,%s,%s,%s,%s,%s,%s,%s\n", genutil.StrTernary(skiprow, "!", ""), mybox, dmidecode1.Csv(), df.Csv(), md.Csv(), scsi.Csv(), parted.Csv(), smartctl.Csv(), tgtd.Csv(), blkid.Csv())
				}
			}
		}
	}
	ofile, hpfile := "", ""
	switch {
	case opt.Odir == "":
		ofile, hpfile = "/dev/stdout", "hp.dbg"
	default:
		ofile = opt.Odir + "/qsdata2." + mybox + ".csv"
		hpfile = opt.Odir + "/hp." + mybox + ".csv"
	}
	genutil.WriteStringToFile(ostrVerbose+ostr, ofile)
	if len(hpstr) > 0 {
		genutil.WriteStringToFile(hpstr, hpfile)
	}
}
