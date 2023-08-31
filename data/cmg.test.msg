mpeg2dec : Revert ranking switch with ffdec_mpeg2video . Fixes # 574461
rdtdepay : make buffer metadata writable before setting caps on it
df : Skip final '/' character when calculating basename .
* kdb . h ( krb5_def_promote_db ): Declare
crypto : fix memory leak in ECDH :: SetPrivateKey
bb # 11514 - check packSizes prior to dereference
fix NULL dereference when running with a limited db
fix write () on a file desriptor of - 1 , bb # 5784
Fix a election calculation
decrease the repeating of the same log
cib : Memory leak - Free query matches in update_attr ()
odb : mempack : fix leaking objects when freeing mempacks
fileops : do not overwrite correct error message on mmap
refs : fix a memory leak
git_connect_git (): forbid newlines in host and path
range - diff : right - trim commit messages
get_sha1_oneline : check return value of parse_object
fts : fts_expunge_log_uid_count () returned garbage on nonexistent log .
imapc : Fix memory leak in disconnected state
lib - smtp : server : Fix assertion failure occurring for a second failed BDAT / BURL command .
pop3 - migration : Don ' t even try to match message sizes with pop3_migration_skip_size_check = yes
doveadm backup : Don ' t unnecessarily delete mailbox when handling expunges
lib - storage : Fixed assert - crash caused by previous stats change .
i_getpwnam (): Ignore EINVAL errors silently .
lib - imap - storage : Fixed leaking istream on error conditions .
lib : io_loop_set_current () - do nothing if ioloop doesn ' t change
ssl - iostream : ssl_iostream_destroy ( NULL ) should be a no - op
lib - storage : If chdir ( home ) fails due to EACCES , don ' t log two errors .
ConnectionExists : make sure conn -> data is set when " taking " a connection
mime : properly unbind mime structure in curl_mime_free ().
clarified the URL part based on the problems Martin Hilpert had
Fix a unit test mistake .
Fixed unit test for mom_job_func
Fixed unit test for pbsd_main .
removed memory leak in job_clone ()
Fix a coverity complaint .
topoh : free nbuf
db_postgres : don ' t free pg query structure in store result function
usrloc : re - init sruid struct for each child
mohqueue : remove call from queue if caller does not exist
carrierroute : export MI commands
af_scaletempo : Fix crash in option parsing
vo_opengl : free to - be - added hook on hook array overflow
ao_null : disable latency emulation
NULLing handle on destroy
Bugfix : knc : Perform le32toh on unknown - work nonces
aapts : Update for ACPICA interface change .
cpu : Add missing ' static ' attribute to qemu_global_mutex
vl : Add failure check for SetEvent
ahci : fix buffer overrun on invalid state load
sheepdog : fix snapshot tag initialization
s390 / kvm : Add check for priviledged SCLP handler
qemu - char : report udp backend errors
sparc / leon3 . c : fix memory leak
qcow2 : Reject too large header extensions
QemuOpts : check NULL input for qemu_opts_del
sheepdog : fix compile error on systems without TCP_CORK
CID 12072 - added mssing return
libsmartcols : fix stupid typo
lscpu : avoid double free
libblkid : don ' t gather PART_ENTRY_ * values for binary interface
swapon : missing separator in swapon - s command
Unload the GeoIP db when disabled .
added ignoreSet to rpmRunTransaction ()
don ' t let hdrs [] be sparse
Have to make unspecified - domain be in writable storage .
carddav : don ' t crash on x - fm - otheraccount - member with no userid
dav : zero out index_state
fix to latency monitor reporting wrong max latency
avcodec / fic : clear slice_data
mp3adu : Set the channel layout properly
movenc : Free the buffer returned by url_close_dyn_buffer , regardless of the size
tta : fix off be 1 error in the end detection .
adding assert to protect delayed_pic
rl2 : Avoid a division by zero
adpcmenc : Do not set coded_frame -> key_frame .
force sample for qcelp when not stored in mov , fix # 968
fixing interlaced dct with 4mv
remove unnecessary shift
rtpdec_vp9 : Make sure to free the temp buffer on close
id3v2 : add a mimetype for bmp pictures .
libopenh264enc : Return a more sensible error code in some init failure paths
rtpenc_chain : Use the original AVFormatContext for getting payload type
h264 - mt : fix deadlock in packets with multiple slices ( e . g . MP4 ).
twinvq : validate that channels is not <= 0
fixing MVs in hq mode
vc1 : Do not return an error when skipping b frames
Removes misleading const qualifier , gets rid of two compiler warnings
http : Always allow no - op seek
avidec : fix a memleak in the dv init code .
hlsenc : use the basename to generate the list entries
disconnect client if trailer is sent
don ' t close a nonexistent connection ( null dereference )
Added support for password - protected Jabber chatrooms .
imcb_file_send_start : handle ft attempts from non - existing users
Avoid a possible dereference of null pointer .
Qt : fix use of uninitialized variable
avoid a crash when tree is null
Extract the center frequency from Peek tagged files .
RADIUS : Initialize vendor attribute type to zero
Qt : add initializer ( CID 1230901 ).
Fixing possible remote overflow due to mbstring translation .
Fixed NULL pointer dereferencing
Fixed a memory leak inside imap_utf8 ()
Added missing charset .
set default response code to 200
Added an E_NOTICE when precision value is truncated
helper : Allow exec on the tmpfs .
systemctl : add missing OOM check
ude_rules . c : fix possible NULL pointer dereference in get_key ()
install : don ' t choke on dead links
systemctl : fix check for template units
resolved : don ' t override zone item state after starting the probe
core / cgroup : first print then free
sd - bus : correct size calculation in DBus fd receive
silence " comparison between signed and unsigned "
Drop the DH group check to a weaker version of the checks
Allow the creation of 48 bit primes
catch error code
allocate cleam memory for cipher to play with
avoid double free , coverity # 153
kadmin : del_enctype check for bogus keys
Fixed memory leak .
xfreerdp - client : xi tweak2
core : free polyline and polygon_sc points
Fixed memory leak .
Fixed # 6009 : Bounds checks in autodetect_recv_bandwidth_measure_results
libfreerdp - core : server fix sending zero length update pdu .
codec : Another realloc fix
inclode strerror in recv error message MINOR
remove unit test spam from mongos
Added missing default initializer for fDirector
af_aresample : check ff_get_audio_buffer () failure
ffplay : fix type of time_diff in waveform display
avformat / yuv4mpegdec : fix " warning : field_order may be used uninitialized in this function "
lavf / tee : fix leak of bsfs array .
r3d : fix an invalid read introduced in 6bf4c1d
wmavoice : Check for out of bound writes
pngdec : check for bprint finalization sucess on icc data parsing
libavformat / mxfdec : fix MXFPackage -> comment_refs memory leak
ffprobe : check av_frame_alloc () failure .
gifdec : gif_copy_img_rect : Fix end pointer
vformat / tee : fix uninitialized use of ret
cmdutils : use sws_freeContext () instead of av_freep ().
ac3_fixed : fix out - of - bound read
avcodec / movtextdec : Call mov_text_cleanup () on close
avformat / concatdec : Enable auto_convert by default
qt - faststart : Avoid leaking memory if encountering a file with double ftyp atoms
pthread : don ' t increment index on zero - sized packets .
avcodec / shorten : Sanity check maxnlpc
bmp : Fix warning X may be used uninitialized in this function
avcodec / dcadec : Check that the added xch channel isnt already there
mpeg4videodec : Fix " warning : dc_pred_dir may be used uninitialized in this function "
fix incorrect check for abs_diff_pic_num overflow
ffmpeg : check avpicture_get_size () retuen value
dsicinav : Check for out of bounds reads
asfdec : set nb_streams to 0 in the asf_read_close
tta : fix off be 1 error in the end detection .
jpeg2000 : Make nreslevel fields int
avcodec / mpeg12dec : Check for overread in mpeg_decode_slice ()
avcodec / alac : only set * got_frame_ptr when all channels have been decoded
lavfi / scale : allocate interlaced scalers only if needed .
avcodec / jpeglsdec : assert that overflows end at exactly x = w
mpegaudiodec : Validate that the number of channels fits at the given offset
vaapi_h264 : Scale log2_max_pic_order_cnt_lsb with max_b_frames
lavd / alsa - audio - common : mark default device in device list
Fix a crash caused by a null coded_picture pointer in ffserver .
librtmp : append the correct field to the string
flashsv2_prime : check block before using it .
audemux : Check channels isnt 0
avcodec / extract_extradata_bsf : Fix leak discovered via fuzzing
swscale : fix another integer overflow at large dimensions / rescales .
avcodec / hevc : use av_mallocz () for allocating tab_ipm
ffplay : Fix usage of private lavfi API
avformat / ipmovie : check OPCODE_INIT_VIDEO_BUFFERS size more completely
avformat / icodec : reduce score returned on probing
avcodec / mjpegdec : zero gb to silence warning about it being possibly uninitialized
avcodec / ituh263dec : Use 0xffff as error code in h263p_decode_umotion ()
ffplay : fix mem leak when opening input or parsing options fail .
libtwolame : prevent a NULL pointer dereference
libavutil / opencl : fixed uninitialized var warning
h264 - mt : fix deadlock in packets with multiple slices ( e . g . MP4 ).
h264 : Check that the codec isn ' t null before accessing it
ffprobe : fix value_string () prefix printing for values with negative logarithm
ffplay : check return code of avcodec_decode_video2 ()
avoid integer overflow in dnxhd encoder , fixes # 1557
av_find_stream_info (): set subtitle codec time_base
ffplay : disallow seeking before the start of the file
avcodec / dirac_dwt : initialize support value for fidelity wavelet
avcodec / mpegvideo : fix null pointer dereference
avformat / asfdec_f : Assert that packet positions match in asf_read_pts ()
ffmpeg_vaapi : Unreference global device before setting it
lavc / ffjni : fix uninitialized variable
Set start_time to 0 for raw audio files .
sapenc : Free AVStream -> info on cleanup
avcodec / pngdec : mark previous_picture as done on end of decode_frame_common ()
avformat / utils : avoid overflow in compute_chapters_end () with huge durations
matroskadec : Fix buffer overread in matroska_ebmlnum_uint
avcodec / wavpack : Fix integer overflow in wv_unpack_stereo ()
parser : fix large overreads
wma : fix off - by - one in array bounds check .
avformat / movenchint : use realloc_array () to protect against integer overflow
Fix out of bound reads in the QDM2 decoder .
swscale : enforce a minimum filtersize .
diracdec : avoid overflow of bytes * 8 in decode_lowdelay
flacdec : Silence false positive warning about uninitialized variables in
avcodec / xan : Check for bitstream end in xan_huffman_decode ()
yadif : check input video size .
avformat / ftp : Solve a crash bug when network occur a exception
avformat / segment : Pass flags to child context
idcin : check for integer overflow when calling av_get_packet ()
libavcodec / util : Fix timebase overflow check
matroskadec : set cur_dts after seek
avformat / omadec : only compute timestamps based on bitrate if its set
avcodec / hevc : Dont turn 32bit timebases into negative numbers
checkasm : v210 : Fix array overwrite
Avoid a null pointer dereference on oom in the aac encoder .
lagarith : avoid infinite loop in lag_rac_refill ()
ffprobe : free shared writer context on exit
Correcting wrong looking stream_id validity check in avidec .
avcodec / mpeg4videoenc : Use 64 bit for times in mpeg4_encode_gop_header ()
indeo4 : fix out - of - bounds function call .
avcodec / diracdec : Fix Assertion frame -> buf [ 0 ] failed at libavcodec / decode . c : 610
avcodec / h264_refs : Fix long_idx check
avutil / mathematics : Fix division by 0
mpegaudiodec : fix short_start calculation
mxfdec : Include FF_INPUT_BUFFER_PADDING_SIZE when allocating extradata .
libfaac : Fix assignments in if ()
flvdec : Fix compiler warning for uninitialized variables
avcodec / snowdec : Check for remaining bitstream in decode_blocks ()
avcodec / lagarith : reenable buggy lag_decode_zero_run_line ()
avformat / mpegenc : Better heuristic for ordering packets
avprobe : Unref the packet once it is used
vfwcap : support MJPG compressed streams .
huffyuv : error out on bit overrun .
avcodec / mpegvideo : make mc_mb_var_sum_temp / mb_var_sum_temp 64bit
avcodec / samidec : check av_strdup () return value
avformat / dsfdec : set bit_rate , fixes duration estimation
indeo4 : Skip null frames .
h2645_parse : don ' t overread AnnexB NALs within an avc stream
Do a little more checking .
lavu / error : fix sign error in av_sterror () error code
avformat / iff : shrink packets to the initialized data
avcodec / xwddec : Check bpp more completely
wmalosslessdec : fix mclms_coeffs * array size
avcodec / wavpack : Fix undefined integer negation
mxfdec : return error if no segments are available in mxf_get_sorted_table_segments
avformat / hlsenc : Fix a memory leak when http_persistent is 1
avcodec / hevc_filter : Fix invalid shift
avcodec / pngenc : Check that there is at least 1 frame
mp3enc : fix signed C99 overflow
qdm2 : fix signed multiplication overflow
lavf / rtsp : Fix a crash with the RTSP muxer .
lagarith : check count before writing zeros .
msmpeg4 : Don ' t set up run - level info for level 0 .
vaapi_h264 : Scale log2_max_pic_order_cnt_lsb with max_b_frames
ffmpeg : fix uninitialized variable warning
vf_scale : don ' t leak SWS context .
msmpeg4enc : fix possible use of uninitialized warning
lavf / swf : Fix auto - detection of compressed files .
mpegvideo_enc : initialize the encoding context
libx264 : fix double free
lavfi / hue : fix crash when resetting the saturation expression
avcodec / cavsdec : Fix undefined behavior from integer overflow
* fix double free
asfenc : fix leaking asf -> index_ptr on error
flvdec : Inject in stream extradata , into the stream .
ffv1 : check for malloc failure .
avcodec / mlpdec : Fix runtime error : left shift of negative value - 22
aaccoder : prevent crash of anmr coder
avformat / movenc : Write version 2 of audio atom if channels is not known
escape124 : reject codebook size 0
matroskadec : fix potential mem leak
swfdec : check lens validity
vc1dec : prevent v_edge_pos from becoming negative .
lavf / vf_framerate : Fix frame leak when increasing framerate .
avcodec / dirac_arith : fix integer overflow
idroqdec : fix leaking pkt on failure
rtsp : Don ' t use uninitialized data if there are no streams
rtsp : Fix a crash with the RTSP muxer
libopencore - amr : check return value of avcodec_alloc_frame ()
applehttp : fix variant discard logic
libavutil / hwcontext_dxva2 : Add check for possible errors from GetAdapterDisplayModeEx
rmdec : fix crash at end of file
fix mem leak
swscale - example : Don ' t return random value from main ().
mlpdec : dont leave a invalid huff_lsb in the context .
pan : fix uninitialized channel_id variable .
avcodec / diracdec : Fix integer overflow in divide3 ()
avcodec / utils : Align YUV411 by as much as the other YUV variants
mxfdec : Fix files with essence containers larger than 2 GiB .
tiff : check bppcount
avcodec / avpacket : Fix memory allocation failure check
avcodec / dnxhddec : Do not overwrite colorspace if the container has set it .
targa : prevent integer overflow in bufsize check .
matroska : Clear prev_pkt between seeks .
avcodec / cfhd : Fix undefined shift
avcodec / bitstream : assert that no integer overflow happened when writing codes in build_table ()
avcodec / takdec : Fix runtime error : left shift of negative value - 360
xbmdec : fix off by one error in scanf ()
avformat / hlsenc : Free context after hls_append_segment
avcodec / h264 / find_start_code : factorize addition out
lavc / cdxl : clear palette before reading it
cbs_h264 : Fix memory leak in error case
avformat / isom : zero MPEG4AudioConfig to avoid leaving fields uninitialized
mp3on4 : fix null pointer dereference
avformat / rmdec : check that sub_packet_size fit in the packet for DEINT_ID_GENR
fraps : check for overread .
movtextenc : Remove dangling reference to movtext . h
avconv : initialize hwaccel_pix_fmt
lavf : prevent crash in av_open_input_file () if ap == NULL .
avcodec / ffv1dec : Clear quant_table_count if its invalid
avformat / rmdec : Fix use of uninitialized variable
check for malloc failure
avcodec / cfhd : Fix invalid left shift of negative value
dont reduce the min_distance of AVIndexEntry
g723_1 : fix overflow in square_root ()
vfwcap : Add support for UYVY pixel format .
smc : fix the bounds check
vf_overlay : ensure the overlay frame does not get leaked .
indeo5 : Fix null pointer dereference .
ffmpeg : avoid possible undefined behavior
mjpegdec : rgb mode is specific for ljpeg , disable it for others .
flashsv : check for keyframe before using differential coding
hlsproto : fix integer overflow
avcodec / apedec : tmpk == 32 is not supported , prevent undefined behavior
mov : Free an earlier allocated array if allocating a new one
qcelpdata : correct start bit for cbgain [ 0 ] in qcelp_rate_octave_bitmap []
avcodec / pictordec : Check plane value before doing value / mask computations
avformat / mux : Check that deinit is set before calling it
aac : Relax reserved_bit validation
Fix a small memory leak
bink : Check for out of bound writes when building tree
Adds _PATH_RSH for rsh - using rcmd ()
uhci : alloc can ' t fail , drop check .
qemu - sockets : do not test path with access () before unlinking
qcow2 : Fix new L1 table size check ( CVE - 2014 - 0143 )
usb - bot : Drop not needed " allow_hotplug = 0 "
acl : Fix acl_remove not to mess up the ACL
e1000e : Fix build with gcc 4 . 6 . 3 and ust tracing
block : Fix anonymous BBs in blk_root_inactivate ()
QemuOpts : check NULL input for qemu_opts_del
ide : remove undefined behavior in ide - test
ppc / spapr_drc : fix memory leak
vhost - user : disable chardev handlers on close
virtio - net : don ' t touch virtqueue if vm is stopped
fsdev : fix memory leak in main ()
ivshmem : remove redundant assignment , fix crash with msi = off
dma - helpers : Initialize DMAAIOCB in_cancel flag
vvfat : Fix segfault on write to read - only disk
s390 / sclpconsole : prevent char layer callback during initialization
esp : delay Transfer Information command if dma is not enabled
device_tree : Fix potential memory leak
pvpanic : use get_uint () for " ioport " property
loader : fix a file descriptor leak
cpu : verify that block -> host is set
fix segfault in setting migration speed
qcow2 - bitmap : fix bitmap_free
acpi - build : disable with - no - acpi
qiov : prevent double free or use - after - free
qbus : fix memory leak in qbus_free ()
throttle : Assert that bkt -> max is valid in throttle_compute_wait ()
pseries : Fix debug message for out - of - bounds address in H_PUT_TCE
i386 : kvmvapic : initialise imm32 variable
target - i386 : ROR r8 / r16 imm instruction fix
hw / isa / pc87312 : Mark the device with user_creatable = false
tests : fix ipmi - bt - test leak
x86 / cpuid : Tighten parsing of tsc_freq = FREQ
load_image_targphys () should enforce the max size
hw / arm / allwinner - a10 : Mark the allwinner - a10 device with user_creatable = false
powerpc pci : fixed packing of ranges []
lm4549 : Fix buffer overflow
virtio : error out if guest exceeds virtqueue size
ARM : ACPI : fix the AML ID format for CPU devices
intc : i8259 : Convert Array allocation to g_new0
block : check for existing device IDs in external_snapshot_prepare ()
qdev : Use qdev_get_device_class () for - device < type >, help
scsi : Fix scsi_bus_legacy_add_drive () scsi - generic with serial
scsi - disk : release AioContext in unaligned WRITE SAME case
replay : fix error message
tests : fix virtio - 9p - test leaks
target - arm : resource leak fixes for iwmmxt disassemble
block : Handle flush error in bdrv_pwrite_sync ()
vhost - user : print original request on error
target - i386 : Check for host features before filter_features_for_kvm ()
mips_mipssim : fix use - after - free for filename
iothread : Make iothread_stop () idempotent
iothread : check iothread -> ctx before aio_context_unref to avoid assertion
qdev : Check if unplug handler exists before calling it
qemu : msix nit : clear msix_entries_nr on error
vl : Add failure check for SetEvent
usb : ehci : fix memory leak in ehci_process_itd
blkverify : Fix leak of opts in blkverify_open
xen_disk : mark ioreq as mapped before unmapping in error case
cpu_physical_memory_write_rom () needs to do TB invalidates
qemu - option : Make qemu_opts_foreach () accumulate return values
spapr_vscsi : Set uninitialized variable
qemu - img : Plug memory leak in convert command
hw / arm : Mark the " fsl , imx25 " device with user_creatable = false
hw / mips_malta : remove redundant irq and clock init
fixed memory leak
KVM : Use return value for error print
qdev - monitor : fix segmentation fault on qdev_device_help ()
linux - user : Fix locking order in fork_start ()
rtl8139 : check IP Total Length field ( CVE - 2015 - 5165 )
target - ppc : fix nego and subf * o instructions
vnc : sanitize bits_per_pixel from the client
posix - aio - compat : Plug memory leak on paio_init () error path
e1000e : correctly tear down MSI - X memory regions
slirp : fix guestfwd id
vhost - user - bridge : fix resume regression ( since 2 . 9 )
virtio - net : fix buffer overflow on invalid state load
input : don ' t queue delay if paused
hw / acpi / aml - build : Fix memory leak
qcow2 : don ' t leak buffer for unexpected qcow_version in header
hw / arm : Mark the " fsl , imx6 " device with user_creatable = false
sheepdog : don ' t leak socket file descriptor upon connection failure
hw / pci : fixed error flow in pci_qdev_init
qdev : Change Property :: offset field to ptrdiff_t type
vnc : don ' t crash getting server info if lsock is NULL
tcg : Reload local variables after return from longjmp
vt82c686 : avoid out - of - bounds read
qemu - sockets : Fix buffer overflow in inet_parse ()
hw / scsi / spapr_vscsi : Remove superfluous memset
hw / misc / mmio_interface : Return after error_setg () to avoid crash
vfio / pci : Fix regression in MSI routing configuration
sockets : avoid crash when cleaning up sockets for an invalid FD
spapr_cpu_core : drop reference on ICP object during CPU realization
cpus . c : add additional error_report when ! TARGET_SUPPORT_MTTCG
block : vhdx - add error check
spapr : Move memory hotplug to RTAS_LOG_V6_HP_ID_DRC_COUNT type
tests : fix ipmi - kcs - test leak
exec : Add missing rcu_read_unlock
ivshmem : Fix fd leak on error
qcow2 : Fix grow_refcount_table error handling
parallels : drop check that bdrv_truncate () is working
blockdev : reset werror / rerror on drive_del
hw / arm_gic . c : Define . class_size in arm_gic_info TypeInfo
fix oob in dex
missing list close
Correcting a comment .
Fixed huge cpu usage when the window is hidden .
Add a getter for need_layout_ .
Fixed type of temporary variable in _TIFFCheckMalloc () as per bug
fix unchecked malloc result
install : explicitly return 0 on success
core / dbus - execute : actually set PassEnvironment = (# 7510 )
shared : fix search_and_fopen with alternate roots
libudev : monitor - export MAJOR / MINOR only if available
fstab - util : detect out - of - range pri = assignments
base - filesystem : avoid all searching if the link already exists
cgroup : properly determine cgroups zombie processes belong to
core / mount : skip incomplete mountinfo entries
journalctl : don ' t choke on entries with no MESSAGE = field
bus_open leak sd_event_source when udevadm trigger 。
[ windows ] init locale_dir only once
[ presentation ] Fix transition of pages with duration = 0
app : skip keyboard devices so the device list becomes reasonable
libgimpconfig : support $( gimp_installation_dir ) in config paths
plug - ins : fix memory leak in edge - neon
Bug 663582 - Brush outline is too slow
Bug 701120 : Opening fuzzed . pnm - file causes stack buffer overflow
[ Indic ] Disable ' kern ' in Uniscribe bug compatibility mode
Fixed a typo , which prevented non - valgrind build .
branches / 5 . 1 : Use btr_free_root () instead of fseg_free () for
branches / zip : Check for autoinc next value overflow explicitly when offset
Applying InnoDB snashot 5 . 0 - ss5406 , part 1 . Fixes BUG # 38479
branches / zip : btr_cur_optimistic_insert (): If btr_page_reorganize ()
fix for bug # 212
Fix an off - by - one buffer size in dirserv . c that magically never hit our
Oct 19 13 : 27 : 24 . 391 [ info ] router_reload_networkstatus (): Skipping cached - status file with unexpected name "."
Harmless : Z_OK is not an acceptable answer to Z_FINISH .
Added < utility > based on C ++ 11 for R - values and std :: move
wow - 2016 now
svn merge - r16071 : 16073 https :// eris . llnl . gov / svn / slurm / branches / slurm - 1 . 3
Prevent slurmctld from dumping core ifjob_resrcs is missing in the
check for NULL pointer
don ' t free any static memory
[ sgen ] We must do bridge processing with all sgen locks taken .
[ sgen ] Fix names of descriptor types in describe_ptr ().
Replace a g_assert with a g_error .
make sure destructors are called , cleaning up socket
add a dropAction () creator for Lua
Added code to randomize values returned by rand ()
list - view : don ' t crash when expanding a directory
window - slot : fix formatting
fixed gcc warning
fix for v1 infe ( MPEG conformance file )
use path only if it is actually present
atr table reallocation fix by William Wanders
gio : changed default symbols
card - westcos . c : fixed potential resource leak
added a check for CKA_CERTIFICATE_TYPE to attr_extract ()
[ Fix ] Simplexmlelement should throw an exception rather than fatal
[ XrdCl ] Treat empty redirect response as error .
chanserv / sync : Return after kicking for RESTRICTED
anope_convert : Avoid a crash on a nickless account .
fixed timer order .
m_ssl_gnutls Only generate DH params when dh_params is inited
Added Buffer member
linux : Make modal dialog show more smoothly
zebra : stack overrun in IPv6 RA receive code ( CVE - 2016 - 1245 )
* src / augeas . c ( init_root ): check for failure of strdup
make sure to unpack the nonce that was calculated .
pluto : undo bug introduced in 0395c26ac17af9b02e4f1b84d7279288fc985ced
ikev2 : reject SKF payloads when the remote never negotiated them
LSW_FDMASK : Avoid signed integer overflow
ikev2 : during Child SA rekey negotiation abort when ESP proposals mismatch .
xkbcomp : Don ' t falsely promise from ExprResolveLhs
fix crash in tracing objtypespec
Added raptor_new_identifier_from_double pointer
Fix a reversed check in upcast_evbuffer
r15245 @ tombo : nickm | 2008 - 04 - 18 09 : 27 : 50 - 0400
http : fix leaking of response_code_line
X3F : check for data offset limit
ssh : fix ' left ' may be used uninitialized
fix compiler warning
don ' t use curl . haxx . se
kj :: repeat () should support subscript operator .
runtime : remove unnecessary ROUND in alg . c
return correct NID for undefined object
fix potential memory leak when allocation fails
Make it possible to disable STORE .
Added a check for malloc fail .
Bugfix : off - by - one in priority calculation
support force termination in script engine
imudp : add error msg when listener wasn ' t created
Fixed bug in IRC plugin when receiving data from server
erts : Search for heart in ports that are alive
trivial : close inotify_fd on exec
src / aiff . c : Fix a buffer read overflow
Specialize the scrollbars for each platform
Plugging memory leak in write_unicode
scrolling in empty windows crashed
mbedtls : print warning if random personalisation fails
check if connection was accepted before logging
dnslookup : missing active count inc
remove a debug message ; appears far too often in normal use
Closing connection after mg_send_digest_auth ()
make sure ' length ' is >= 0 in afc_file_read
jpc_t1cod : check whether passno is in range
BUG : porting bugfix from refactor @ 3da77df
Check the return value of fscanf () when reading a PID file .
x86 : instruction length must be <= 15
mds : drop auth pins before waiting for dir unfreeze
mds : Keep journaler in readonly mode until replay completes .
librbd : add create_async_context_callback helper
cls_rgw : fix rgw_bucket_init_index
rbd : disk usage on empty pool no longer returns an error message
osd : gracefully exit when create_object_store () returns NULL
rgw : fix a typo
osd : fix uninit value in scrub message
mds : update current_parent_since for newly snapshotted directory
osd_types : add missing osd op flags
kclient : setxattr for virtual xattr is not allowed
add ceph_fsid_set helper
librados : Allow rados_pool_list () to get NULL buffer if len == 0
kclient : fix NULL dereferencing oops
fix FOLDS_FOLD_ADDR_BUK () for remove warning .
set_fat (): Fix off - by - 2 error leading to corruption in FAT12
fix type mismatch anywhere int != int32 ( Vorne )
Reduced the max memory use and image size in 64 - bit builds
Fixed a GIF decoding bug ( divide by zero )
made suidrt . c work
fix a used - before - set error in im_vips2dz
ipf : release unhandled packets from the batch
Acquire the trx_sys_t :: lock before traversing the trx list .
Fixed a bug in vis . c
Edited fix for # 1224 .
Fixed error in merge
x86 : properly initialize temp insn buffer for paravirt patching
Input : ati_remote - add missing input_sync ()
mtd : nand_bbt : set the smallest size of bbt table
drm / radeon / kms / evergreen : No EnableYUV table
s390 / con3270 : testing return kzalloc retval
xen / pciback : Check PF instead of VF for PCI_COMMAND_MEMORY
xen - gntalloc : integer overflow in gntalloc_ioctl_alloc ()
regmap : Don ' t attempt block writes when syncing cache on single_rw devices
igmp : Avoid zero delay when receiving odd mixture of IGMP queries
[ SCSI ] zfcp : prevent fc_remote_port_delete calls for unregistered rport
ARM : OMAP : Initialize PWM backlight enable_gpio field
staging : android : logger : enforce GID and CAP check on log flush
[ SCSI ] qla2xxx : Don ' t hardcode fw_transfer_size for ISP2 [ 45 ] xx parts .
kexec / uefi : copy secure_boot flag in boot params across kexec reboot
cfg80211 : reset auth algorithm
iwlwifi : mvm : allow RSSI compensation
posix - timer : Don ' t call idr_find () with out - of - range ID
tracepoints : fix disable
x86 : Fix panic when handling " mem ={ invalid }" param
[ media ] davinci : vpbe : fix build error when CONFIG_VIDEO_ADV_DEBUG is enabled
x86 / hpet : Check for irq == 0 when allocating hpet MSI interrupts
xfs_file_last_byte () needs to acquire ilock
ath10k : don ' t call quiet mode if it ' s not implemented
fs : fix data - loss on error
hwmon : ( w83793 ) Fix vrm write operation
bna : Check for NULL before deref in bnad_cb_tx_cleanup
pch_phub : Fix MAC address writing issue for LAPIS ML7831
KVM : i8259 : simplify pic_irq_request () calling sequence
ioat : fix type mismatch for -> dmacount
[ SCSI ] ibmvscsi : Correct DMA mapping leak
fb_defio : Push file_update_time () into fb_deferred_io_mkwrite ()
[ SCSI ] be2iscsi : memset wrb for ring create
[ BNX2 ]: check return of dev_alloc_skb in bnx2_test_loopback
ieee802154 : 6lowpan : Drop PACKET_OTHERHOST skbs in 6lowpan
ALSA : hda - Remove superfluous pcm NULL check
tools / firewire : nosy - dump : work around segfault in decode_fcp
drivers / net / eql . c : prevent reading uninitialized stack memory
KVM : VMX : Fix pending NMI - vs .- IRQ race for user space irqchip
irda : Fix irda_getname () leak
[ PATCH ] janitor : reiserfs : super . c - vfree () checking cleanups
drivers / net / cpmac . c : Correct code taking the size of a pointer
ath5k : fix NULL pointer in antenna configuration
taskstats : cgroupstats_user_cmd () may leak on error
sh : Zero - out coherent buffer in consistent_alloc ().
usb : gadget : uvc : memory leak in uvcg_frame_make ()
iio : kfifo_buf improve error handling in read_first_n .
drm / nouveau : Do not leak client objects
ath5k : free sc -> ah in ath5k_deinit_softc
USB : xhci : No GFP_KERNEL in block error handling
perf : Fix inherited events vs . tracepoint filters
x86 : fix printout ugliness in cpu info printk
staging : comedi : cb_pcidas : check for failure of subdev_8255_init
PCI : spear : Fix Section mismatch compilation warning for probe ()
[ PATCH ] frv : fix uninitialised variable in serverworks driver
ipv6 : udp : fix the wrong headroom check
ath9k : Use memcpy in ath_clone_txbuf ()
drivers / serial / uartlite . c : Add missing of_node_put
[ SCSI ] mptfusion : Set fw_events_off to 1 at driver load time .
Fix an OOPS when initializing IR remote on saa7134
Btrfs : Fix balance panic
mwifiex : corrections in PCIe event skb handling
rtnetlink : Fix VF IFLA policy
IPoIB : Free child interfaces properly
Drivers : hv : Fix a memory leak
iwlwifi : fix rx signal quality reporting in dmesg
dm snapshot : fix a possible invalid memory access on unload
mmc : card . h : Use NULL instead of 0 for END_FIXUP
ixgbe : Return PCI_ERS_RESULT_DISCONNECT when bus is disabled
KVM : VMX : Stop invalid guest state emulation on pending event
Staging : Fix leak in drivers / staging / at76_usb . c
ext4 : Fix update of mtime and ctime on rename
drm / i915 : Don ' t kick - off hangcheck after a DRI interrupt
mfd : Check for twl4030 - madc NULL pointer
rocker : print switch ID consistent with phys_switch_id sysfs node
include / asm - x86 / setup . h : checkpatch cleanups - formatting only
ASoC : wm8731 : Check for clk_prepare_enable () error
[ S390 ] cio : fix memleak in resume path
perf session : Don ' t call dump_sample () when evsel is NULL
pwm : omap - dmtimer : Potential NULL dereference on error
ALSA : hda - Force to reset IEC958 status bits for AD codecs
dm : raid fix write_mostly arg validation
ARM : at91 : remove empty at91_init_serial function
ARM : PL08x : prevent dma_set_runtime_config () reconfiguring memcpy channels
mtd : mxc_nand : set owner field to prevent module unloading when in use
rcar - hpbdma : add max transfer size
usb : m66592 - udc : Fixed bufnum of Bulk
[ media ] media : davinci_vpfe : clear the output_specs
[ media ] media : rcar_vin : Reject videobufs that are too small for current format
wl12xx : set max_sched_scan_ie_len correctly
alloc_percpu : fix UP build
[ media ] ir - lirc - codec . c : don ' t leak lirc -> drv - rbuf
dmaengine : k3 : Remove chancnt affectations
ALSA : asihpi - Fix potential Oops in snd_asihpi_cmode_info ()
rt2x00 : Fix memleak in tx () path
sched : Try to deal with low capacity , fix update_sd_power_savings_stats ()
hwmon : ( f75375s ) Disable setting DC fan control mode for F75373
staging : brcm80211 : remove unnecessary cast in wlc_d11hdrs_mac80211
dlm : tcp_connect_to_sock should check for - EINVAL , not EINVAL
drm / mgag200 : check alloc_apertures () success in mga_vram_init ()
[ media ] fc2580 : silence uninitialized variable warning
Btrfs : should add a permission check for setfacl
usb : dwc2 : gadget : ignore stall check for ep0
Input : gamecon - clear unused function pointers
dmaengine : ste_dma40 : Declare memcpy config as static
perf machine : Fix leak of ' struct thread ' on error path
USB : rtl8150_disconnect () needs tasklet_kill ()
Staging : comedi : integer overflow in do_insnlist_ioctl ()
libata - sff : Don ' t call bmdma_stop on non DMA capable controllers
microblaze : rt_sigreturn is too trigger - happy about sigaltstack errors
[ SCSI ] iscsi : nodelay fix
slob : fix free block merging at head of subpage
iwlwifi : mvm : make sure AP is operating for ToF
libceph : remove redundant declaration
[ PATCH ] IB / mthca : Fix memory leak on error path
efi : rtc - efi : Mark UIE as unsupported
watchdog : gef_wdt . c : add missing remove callback
clockevents : export clockevents_unbind_device instead of clockevents_unbind
ath9k_htc : check for underflow in ath9k_htc_rx_msg ()
rtlwifi : rtl8821ae : Fix problem with regulatory information
tpm : Do not dereference NULL pointer if acpi_os_map_memory () fails .
perf bench : Fix NULL pointer dereference in " perf bench all "
[ PATCH ] pcnet32 : remove incorrect pcnet32_free_ring
btrfs : reada : move reada_extent_put to place after __readahead_hook ()
perf : Fix NULL deref
staging : iio : adis16400 : Remove samplerate_available attribute
[ XFRM ]: Fix missed error setting in xfrm4_policy . c
pxa3xx_nand : reset read buffer before reading
sctp : add a checking for sctp_sysctl_net_register
arm64 : KVM : use ' int ' instead of ' u32 ' for variable ' target ' in kvm_host . h .
cifs : integer overflow in parse_dacl ()
drm / i915 : Add a couple WARN ()' s to catch missing locks
ALSA : pcm : don ' t override timestamp unconditionally
staging : et131x : don ' t cast a void * to a struct pointer
iwlwifi : mvm : disable uAPSD due to bugs in the firmware
[ media ] rtl820t : Add a debug msg when PLL gets locked
[ XFS ] Fix hang after disallowed rename across directory quota domains
[ BOND ]: Fix warning in bond_sysfs . c
staging : comedi : ni_660x : use comedi_handle_events ()
USB : fix interface sysfs file - creation bug
Input : pmic8xxx - pwrkey - pass correct pointer to free_irq ()
mm , percpu : Make sure percpu_alloc early parameter has an argument
leds : Fix trigger unregister_simple if register_simple fails
ASoC : 88pm860x - codec : Cleanup manual bias level transitions
dm raid1 : fail writes if errors are not handled and log fails
hwmon : ( k10temp ) Remove duplicate pci - id define
x86 - 64 : Don ' t use pointer to out - of - scope variable in dump_trace ()
genirq : NULL struct irq_desc ' s member ' name ' in dynamic_irq_cleanup ()
drivers : net : cpsw : irq not disabled in cpsw isr in particular sequence
[ SCSI ] bnx2i : Added reconnect fix connecting against Lefthand targets
pkt_sched : sch_generic : Add Qdisc_ops peek () method .
fs / pstore : update the backend parameter in pstore module
[ S390 ] missing check in dasd_eer_open .
drm / i915 : synchronize_irq () before turning off disp2d power well on VLV / CHV
Btrfs : fix race between allocate and release extent buffer .
ide : add hwif -> chipset fixup to ide_device_add ()
sched : Fix incorrect sanity check
ALSA : hda - Add MSI blacklist
sysctl : Support vdso_enabled sysctl on SH .
iio : core : Handle error when mask type is not separate
drm / i915 : make sure panel is sequenced off when starting a mode set
i40e : Add missing braces to i40e_dcb_need_reconfig ()
clocksource : of : Respect device tree node status
virtio : fix suspend when using virtio_balloon
HID : hyperv : match wait_for_completion_timeout return type
drm / radeon / kms : Fix NULL ptr dereference
ipv4 : enable getsockopt () for IP_NODEFRAG
btrfs : Fix memory leak in btrfs_read_fs_root_no_radix ()
rt2x00 : Don ' t initialize MM40 HT protection to RTS / CTS on PCI devices
Btrfs : don ' t panic if orphan item already exists
staging : crystalhd : fix a style warning
[ SCSI ] ibmvfc : Fix DMA mapping leak on memory allocation failure
[ media ] uvcvideo : Fix a " ignoring return value of ‘ __clear_user ’" warning
[ media ] vb2 : fix compiler warning
Staging : agnx : Move a dereference below a NULL test
ALSA : hda - Update automute / automic upon jack retasking
[ media ] uvcvideo : Constify the uvc_entity_match_guid arguments
cpumask : Use nr_cpu_ids in seq_cpumask
md : check for memory allocation failure in faulty personality
[ ALSA ] ASoC - Bit clock matching error
[ media ] w9966 : zero device state after a detach
usb : dwc3 : gadget : ignore endpoint IRQs when endpoint is disabled
mm : alloc_large_system_hash check order
[ PATCH ] hrtimer : call get_softirq_time () only when necessary in run_hrtimer_queue ()
be2net : take care of __vlan_put_tag return value
[ IPV6 ]: Fix crash in ip6_del_rt
perf : Fix perf_event_init_context ()
[ TG3 ]: Fix PHY loopback on 5700
crypto : cts - Weed out non - CBC algorithms
perf bench sched - messaging : Plug memleak
sh_eth : Check for DMA mapping errors on transmit
mtd : lart : initialize writebufsize
USB : atmel_usb_udc : Use kzalloc () to allocate ep structures
tracepoints : use rcu_ * _sched_notrace
mfd : Fix palmas regulator pdata missing
ARM : EXYNOS : Fix potential NULL pointer access in exynos_sys_powerdown_conf
fs / binfmt_misc . c : add terminating newline to / proc / sys / fs / binfmt_misc / status
drm / radeon : fix UVD 256MB check
drm / i915 : Restore lost DPLL register write on gen2 - 4
jffs2 : Add missing capability check for listing trusted xattrs
drm / mga200g : Use lockless gem BO free callback
[ SCSI ] Fix block queue and elevator memory leak in scsi_alloc_sdev
xfs : Fix possible use - after - free with AIO
[ LLC ]: Restrict LLC sockets to root
HID : wacom : fix proximity tool release
userns : Check euid no fsuid when establishing an unprivileged uid mapping
tcmu : return on first Opt parse failure
virtio / 9p : verify device has config space
isdn : guard against a potential NULL pointer dereference in old_capi_manufacturer ()
leds : tca6507 : silence an uninitialized variable warning
ALSA : hdspm - Constrain periods to 2 on older cards
ASoC : arizona : Record FLL setting when disabling
drm / i915 : Use MLC ( l3 $) for context objects
drm / radeon : use fixed PPL ref divider if needed
clk : ti : Check kzalloc () for failures
drm / i915 : Fix intel_tile_width () parameters
Input : gpio_keys - fix a memory leak
knfsd : Fixed problem with NFS exporting directories which are mounted on .
[ TCP ] tcp_probe : use GCC printf attribute
seccomp : cap SECCOMP_RET_ERRNO data to MAX_ERRNO
printk : suppress empty continuation lines
ath9k_hw : print device ID if not supported
drm / sti : sti_vtg : Handle return NULL error from devm_ioremap_nocache
drm / i915 : Avoid NULL ctx -> obj dereference in debugfs / i915_context_info
[ SPARC64 ] sunhv : Support SYSRQ properly .
i40e : stop the service task at shutdown
HID : cp2112 : fix to force single data - report reply
ocfs2 : o2hb : fix double free bug
cxl : Disable SPAP register when freeing SPA
mmc : mmci : fix mmci_post_request
OMAP4 : Fix for CONTROL register Base
[ libata ] ata_piix : add missing kfree ()
ALSA : hdspm - Provide ioctl_compat
fsl / fman : fix error handling
ipv4 : Fix memory leak in exception case for splitting tries
wlcore : increase max roc duration to 30 seconds
perf / x86 : Check perf_callchain_store () error
Btrfs : fix a scrub regression in case of write errors
irqdomain : Export irq_domain_free_irqs_common
drm / amdgpu : no cg for soc15 of SRIOV
hv_netvsc : fix a race between netvsc_send () and netvsc_init_buf ()
[ PATCH ] utsns : fix ! CONFIG_UTS_NS behavior
ACPICA : Fix memory leak in table load error path
lib / genalloc . c : check result of devres_alloc ()
drm / i915 : WARN if module opt sanitization goes out of order
cfg80211 : fix channel setting for wext
[ media ] stb0899 : don ' t go past DiSEqC msg buffer
Input : gpio - keys - report error when disabling unsupported key
Drivers : hv : Fix a memory leak
[ PATCH ] pata_marvell : merge Mandriva patches
usb : renesas_usbhs : gadget : fix NULL pointer dereference in ep_disable ()
drm : mali - dp : Set the drm -> irq_enabled flag to match driver ' s state .
i915 : fix lock imbalance on error path ...
USB : Add device quirk for Microsoft VX700 webcam
[ SCSI ] zfcp : Fix error checking for ELS ADISC requests
mac80211 : mesh only parameter mppath maybe unused
w1 - call request_module with w1 master mutex unlocked
staging : comedi : adv_pci_dio : fix bug in ' detach '
PCI : use pci_is_root_bus () in pci_read_bridge_bases ()
slab : Use proper formatting specs for unsigned size_t
staging : bcm2835 - camera : prefer allocating memory using sizeof (* ptr ) instead of sizeof ( struct type name )
[ S390 ] cio : allow setting boxed devices offline
cfg80211 : minimal error handling for wext - compat freq scanning
jfs : avoid undefined behavior from left - shifting by 32 bits
ath : do not update cycle counters with sleep mode
iio : ak8975 : Add device name
Btrfs : return error code in btrfs_check_trunc_cache_free_space ()
[ media ] media : rcar_vin : Reject videobufs that are too small for current format
staging : ft1000 : fix kernel info leak to userland for statistics
Btrfs : remove dead code
drm : Perform ioctl command validation on the stored kernel values
ARM : OMAP2 : Prepare cpu detection for further improvements
spi : spidev : fix possible NULL dereference
rt2x00 : Shortcut link state updates when not operating as STA
dmaengine : zxdma : Fix off - by - one for testing valid pchan request
mfd : twl4030 - power : Fix hang on reboot if sleep configuration was loaded earlier
drm / i915 / bxt : Use PHY0 GRC value for HW state verification
UBI : always re - read in case of read failures
ASoC : Check for errors when writing WM8731 reset register
drivers / perf : arm_pmu : Avoid leaking pmu -> irq_affinity on error
panic , vt : do not force oops output when panic_timeout < 0
xfs : Use kmem_free () instead of free ()
iio : hid - sensors : Prevent crash during hot - unplug
ARM : shmobile : mackerel : remove OCR masks , where regulators are used
mtd : docg3 : fix BCH registers
KVM : s390 : implement KVM_CAP_NR / MAX_VCPUS
x86 : fix broken flush_tlb_others_ipi ()
[ ALSA ] Return error if no user TLV is defined
cciss : fix reporting of max queue depth since init
ocfs2 : Re - order iput in ocfs2_drop_dentry_lock
ARM : Fix build error for SMP = n builds
sh - pfc : r8a7790 : Fix definition of IPSR5
usbfs : Fix oops related to user namespace conversion .
[ CPUFREQ ] powernow - k8 : get drv data for correct CPU
KVM : nVMX : Remove redundant get_vmcs12 from nested_vmx_exit_handled_msr
Staging : bcm : Remove assignment in if condition reported by checkpatch . pl
[ media ] marvell - cam : power down mmp camera on registration failure
ceph : fix leak of mon authorizer
iio : light : ltr501 bug in parameter sanity check .
iw_cxgb4 : initialize ibdev . iwcm -> ifname for port mapping
i40iw : Add NULL check for ibqp event handler
greybus : uart : Fix the memory leak in connection init
igb : Change memcpy to struct assignment
drm / exynos / fbdev : set smem_len for fbdev
Drivers : hv : vss : run only on supported host versions
[ PATCH ] fix PROMISC / bridging in TLAN driver
[ PATCH ] x86 : initialise tss -> io_bitmap_owner to something
USB : yurex : fix spelling mistake in comment
hfsplus : fix possible deadlock when handling corrupted extents
staging : octeon - usb : Cocci spatch " noderef "
Input : tegra - kbc - correct call to input_free_device
microblaze : Ptrace notifying from signal code
capabilities : initialize has_cap
staging : most : add missing channel initialization
ath9k : Do not use local_bh_disable in ampdu_action
[ PATCH ] i4l : memory leak fix for sc_ioctl ().
drm / atomic : Wait for vblank whenever a plane is added to state .
unix : use wq_has_sleeper in unix_dgram_recvmsg
dsa : mv88e6xxx : Fix error handling in mv88e6xxx_set_port_state
perf trace : Fix detection of script extension
bnx2x : Add known PHY type check
fscrypt : use EEXIST when file already uses different policy
mtd : docg3 : fix debug log verbosity
USB : serial : digi_acceleport : fix OOB - event processing
ASoC : rt298 : fix null deref on acpi driver data
drm / i915 : Don ' t access snooped pages through the GTT ( even for error capture )
drm / i915 / dp : Ratelimit DP aux timeout messages
x86 / PCI : don ' t bother with root quirks if _CRS is used
tcm_fc : Fix crash seen with aborts and large reads
vfs : abort dedupe loop if fatal signals are pending
virtio : console : fix race in port_fops_open () and port unplug
x86 : put trigger in to detect mismatched apic versions
kvm : irqchip : fix memory leak
dma : tegra20 - apbdma : err message correction
r8169 : fix a bug in rtl8169_init_phy ()
kernel / irq / irqdomain . c : before use ' irq_data ', need check it whether valid .
dm cache : do not wake_worker () in free_migration ()
tty : Deprecate ldisc . chars_in_buffer () method
tg3 : Set initial carrier state to off .
[ IPSEC ] Set byid for km_event in xfrm_get_policy
ARM : msm : Fix section mismatches in proc_comm . c
drm : Check if the allocation has succeeded before dereferencing newmode
iio : ad5755 : fix off - by - one on devnr limit check
tracing : Fix off - by - one in trace_get_user ()
[ PATCH ] i2c : i2c - ibm_iic add I2C_CLASS_HWMON
tty : fix race in tty_fasync
mac80211 : remove a bit of dead mesh code
mfd : sta2x11 - mfd : Only add sta2x11_mfd if it hasn ' t already been added
HSI : nokia - modem : fix error handling of irq_of_parse_and_map
ixgbe : in shutdown , do netif_running () under rtnl_lock
PCI : keystone - dw : Update PCI config space remap function
chelsio : error path fix
[ media ] dvb - usb - gp8psk : Fix tuner timeout ( against git )
mfd : Add free max8998 -> ono irq in max8998_irq_exit ()
tracing / function - graph : Use correct string size for snprintf
OMAP : DSS2 : Check if display supports update mode changes
macvtap : fix uninitialized return value macvtap_ioctl_set_queue ()
rfs : call sock_rps_record_flow () in tcp_splice_read ()
staging : greybus : arche - platform : compress return logic into one line
Btrfs : Handle transid == 0 while opening devices
hwmon : ( coretemp ) fix oops on cpu unplug
[ NETFILTER ]: nfnetlink_queue : fix possible NULL - ptr dereference
ALSA : hdsp - info leak in snd_hdsp_hwdep_ioctl ()
f2fs : avoid gc in cp_error case
dm log writes : move IO accounting earlier to fix error path
cgroup : remove redundant code in cgroup_rmdir ()
xfrm : Fix truncation length of authentication algorithms installed via PF_KEY
mwl8k : declare MODULE_FIRMWARE
sh_eth : Check for DMA mapping errors on transmit
tcp : Report mss_clamp with TCP_MAXSEG option in repair mode
pm8001 : Fix to remove null pointer checks that could never happen
ASoC : wm8750 : Replace direct snd_soc_codec dapm field access
i2c : Align i2c_device_id
firewire : core : fix an information leak
scsi : ufs : qcom : Don ' t free resource - managed kmalloc element
crypto : talitos - Fix timing leak in ESP ICV verification
rtlwifi : rtl8192cu : Fix unitialized struct
xfs : fix memory leak in xlog_recover_add_to_trans
USB : OHCI : don ' t allocate HCCA atomically
parisc : fix wrong page aligned size calculation in ioremapping code
KVM : use heuristic for fast VCPU lookup by id
airo : return from set_wep_key () when key length is zero
kernel / exit . c : avoid undefined behaviour when calling wait4 ()
ARM : 7185 / 1 : perf : don ' t assign platform_device on unsupported CPUs
eCryptfs : Unlock page in write_begin error path
KVM : MCE : Add MCG_SER_P into KVM_MCE_CAP_SUPPORTED
cdrom : Check SCSI passthrough support before reading audio
ipv4 : fib : Set offload indication according to nexthop flags
HID : i2c - hid : prevent buffer overflow in early IRQ
iio : mxs - lradc : fix buffer overflow
pNFS / flexfiles : missing error code in ff_layout_alloc_lseg ()
cifs : ensure that vol -> username is not NULL before running strlen on it
USB : usbmon : use the new usb debugfs directory
drm / amdkfd : Pass queue type to pqm_create_queue ()
drm / amd / display : Disable cursor on video surface .
strip : Fix termios assumption
agp : info leak in agpioc_info_wrap ()
omap3 : Fix booting if package is uninitialized
KVM : Don ' t accept obviously wrong gsi values via KVM_IRQFD
dmaengine : ioatdma : workaround SKX ioatdma version
mwifiex : corrections in PCIe event skb handling
virtio_net : zero is an invald queue_pairs number
misc : apds9802als : Fix the logic checking timeout in als_wait_for_data_ready ()
scsi : libsas : fix memory leak in sas_smp_get_phy_events ()
USB : serial : fix error message on close in generic driver
Btrfs : add missing error handling to read_tree_block
ext4 : set h_journal if there is a failure starting a reserved handle
rndis_wlan : add return value validation
hugetlb : fix race condition in hugetlb_fault ()
lpfc : Fix internal loopback failure .
drm / radeon : only apply the SS fractional workaround to RS [ 78 ] 80
scsi : fcoe : fix use - after - free in fcoe_ctlr_els_send
tpm : Fix initialization of the cdev
powerpc / radix : Update MMU cache
drm / i915 : get / put runtime PM when we get / put a power domain
caif : Fix missing msg_namelen update in caif_seqpkt_recvmsg ()
mfd : rtsx : Release IRQ during shutdown
sound / soc / fsl / p1022_ds . c : add missing of_node_put
[ SCSI ] be2iscsi : fix memory leak on error path
udf : avoid info leak on export
x86 / microcode / AMD : Drop redundant printk prefix
KVM : powerpc : fix init / exit annotation
drm / omap : verify that display x - res is divisible by 8
ip6erspan : make sure enough headroom at xmit .
pinctrl : s3c24xx : off by one in s3c24xx_eint_init ()
staging : rtl8188eu : use eth_broadcast_addr () in place of memset
aio : fix kernel memory disclosure in io_getevents () introduced in v3 . 10
amd - xgbe : fix potential memory leak in xgbe - debugfs
staging : rtl8192u : check dev_alloc_skb () return value in fw_download_code ()
net : avoid NULL deref in inet_ctl_sock_destroy ()
ocfs2 : fix memory leak in dlm_migrate_request_handler ()
drivers : misc : pass miscdevice pointer via file private data
usb : misc : legousbtower : Fix memory leak
xen / events : prevent calling evtchn_get on invalid channels
mac80211 : lock sdata in ieee80211_csa_connection_drop_work ()
ARM : OMAP2 +: make omap_dma_dev_info __initdata
ubifs : Use kmalloc_array ()
staging : typec : tcpm : Check cc status before entering SRC_TRY_DEBOUCE
chipidea : udc : free pending TD at removal procedure
fscrypto : only allow setting encryption policy on directories
staging : ozwpan : Check error condition before creating endpoint .
ARM : pxa : Improve a size determination in pxa3xx_u2d_probe ()
ath9k_hw : fix an off - by - one error in the PDADC boundaries calculation
[ POWERPC ] Improve robustness of the UIC cascade handler
usb : udc : core : fix error handling
staging : comedi : ni_labpc : fix a > 80 char line issue
[ Bluetooth ] Ignore additional interfaces of BPA 100 / 105 devices
watchdog : imx2_wdt : also set wdog -> timeout to new_timeout
powerpc / spufs : Remove invalid semicolon after if statement
KVM : x86 : fix for buffer overflow in handling of MSR_KVM_SYSTEM_TIME ( CVE - 2013 - 1796 )
mqueue : fix a use - after - free in sys_mq_notify ()
dac960 : return success instead of - ENOTTY
eCryptfs : Remove buggy and unnecessary write in file name decode routine
drm / i915 : Defend against userspace creating a gem object with size == 0
mm : add ' try_get_page ()' helper function
amd - xgbe : Update connection validation for backplane mode
staging : android : Break up a long line in binder_send_failed_reply
crypto : qat - update init_esram for C3xxx dev type
blk - mq : fix allocation of set -> tags
ASoC : DaVinci : pcm , constrain buffer size to multiple of period
ALSA : Fix a Oops bug in omap soc driver .
iommu : smar : Fix return value check of create_irq ()
iwlwifi : mvm : fix possible memory leak
ath10k : enable channel 144 on 5GHz band
mwifiex : sdio : fix use after free issue for save_adapter
mtd : nand : sharpsl : Register partitions using the parsers
ALSA : sound / isa / opti9xx / miro . c : eliminate possible double free
TOMOYO : Fix oops in tomoyo_mount_acl ().
ath9k : Fix TX power calculation
module : Don ' t report discarded init pages as kernel text .
mdio_bus : Fix use - after - free on device_register fails
drm / amd / display : fix uninitialized variable warning
PCI : Don ' t leak memory if sysfs_create_bin_file () fails
drm / i915 / gvt : add enable_execlists check before enable gvt
bridge : netfilter : fix information leak
staging : iio : iio_ring_rip_outer return immediately if rip_lots returns <= 0
e100 : make sure vlan support isn ' t advertised on old adapters
greybus : core : add defensive connection disable post disconnect
leds : Use kcalloc instead of kzalloc to allocate array
spi : imx : Gracefully handle NULL master -> cs_gpios
staging : wilc1000 : linux_wlan . c : add kzalloc error check
xhci : replace USB_MAXINTERFACES with config -> desc . bNumInterface
spi : gpio : prevent memory leak in spi_gpio_probe
staging : lustre : fix sparse warning about different address spaces
ASoC : s6000 : Use WARN_ON () instead of BUG_ON ()
usb : return error code when platform_get_irq fails
mtd : nand : mpc5121_nfc : show parent device in sysfs
mfd : sta2x11 - mfd : Only add sta2x11_mfd if it hasn ' t already been added
ip_tunnel : clear IPCB in ip_tunnel_xmit () in case dst_link_failure () is called
[ PARISC ] set memory ranges in N_NORMAL_MEMORY when onlined
[ media ] c8sectpfe : Fix broken circular buffer wp management
video : mmp : fix memcpy wrong size for mmp_addr issue
batman - adv : Print jiffies as unsigned long
mm : numa : Do not account for a hinting fault if we raced
KVM : PPC : Fix oops when checking KVM_CAP_PPC_HTM
[ IPV4 ] fib_hash : fix duplicated route issue
rtlwifi : fix memory leak for USB device
netrom : fix invalid use of sizeof in nr_recvmsg ()
slub : Zero initial memory segment for kmem_cache and kmem_cache_node
iwlwifi : mvm : check if returned value is NULL
bpf : fix double - fdput in replace_map_fd_with_map_ptr ()
spectra : Don ' t overwrite nand_debug_level . It ' s a module param
gpio : GPIO_GET_CHIPINFO_IOCTL : Fix information leak
tty : Fix GPF in flush_to_ldisc ()
inet : properly align icsk_ca_priv
ath5k : free sc -> ah in ath5k_deinit_softc
[ media ] s5c73m3 : Fix entity function assignment for the OIF subdev
powerpc / perf : Fix crash in perf_instruction_pointer () when ppmu is not set
tty : Fix LED error return
ipv6 : rt6_info output redirect to tunnel output
spi : qup : Fix return value checking for pm_runtime_get_sync ()
typhoon : Add missing firmware copy .
cifs : fix off - by - one bug in build_unc_path_to_root
Staging : wilc1000 : Prefer eth_broadcast_addr over memset ()
stmmac : fix noderef . cocci warnings
drm / i915 : Cancel pending execlists irq handler upon idling
powerpc / perf : Cap 64bit userspace backtraces to PERF_MAX_STACK_DEPTH
ixgbe : use kzalloc for allocating one thing
drm / tegra : Correct timeout in tegra_syncpt_wait
mfd : Fix more undefined twl4030 - power resconfig value checks
USB : io_ti : Fix NULL dereference in chase_port ()
[ media ] ov9650 : off by one in ov965x_enum_frame_sizes ()
usb : chipidea : Fix memleak for ci -> hw_bank . regmap when removal
perf / core : Fix potential double - fetch bug
drm / i915 : Discard objects from mm global_list after being shrunk
Staging : sb105x : info leak in mp_get_count ()
staging : usbip : vhci_hcd : fixed suspend - resume loop
greybus : audio_codec : Free gccodec on codec probe failure
[ POWERPC ] arch / powerpc / kernel / vio . c of_node_put cleanup
[ POWERPC ] EEH : hotplug recovery bugfix
pci : Don ' t crash when reading mpss from root complex
ozwpan : divide - by - zero leading to panic
ksmbd : fix use - after - free bug in smb2_tree_disconect
USB : visor : fix null - deref at probe
fbdev : propagate result of fb_videomode_from_videomode ()
dmaengine : imx - sdma : fix up param for the last BD in sdma_prep_slave_sg ()
clk : tegra : cclk_lp has a pllx / 2 divider
mtd : mxc_nand : Propagate the error if platform_get_irq () fails
iio : buffer : Free active scan mask in iio_disable_all_buffers ()
virtio_net : enable big packets for large MTU values
iommu / vt - d : Fix a potential memory leak
mtd : spear_smi : add NULL check on devm_kzalloc () return value
ASoC : Intel : Skylake : fix noderef . cocci warnings
[ SCTP ]: Fix kernel panic while received AUTH chunk while enabled auth
iwlwifi : dvm : disable power save by default
KVM : MTRR : remove MSR 0x2f8
bcache : fix cache_set_flush () NULL pointer dereference on OOM
[ PATCH ] oom : kthread infinite loop fix
ipv6 - multicast : Fix memory leak in IPv6 multicast .
KVM : x86 : Avoid theoretical NULL pointer dereference in kvm_irq_delivery_to_apic_fast ()
s390 / ipl : always use load normal for CCW - type re - IPL
afs : Add missing afs_put_cell ()
ARM : omap - device : add missed callback for suspend - to - disk
[ SCSI ] scsi_lib : use correct DMA device in __scsi_alloc_queue
ext4 : Eliminate potential double free on error path
rtl8xxxu : prevent leaking urb
udp : fix if statement in SIOCINQ ioctl
[ TCP ]: Avoid a divide in tcp_mtu_probing ()
mwifiex : pcie : Fix memory leak in mwifiex_pcie_init_evt_ring
tun : handle register_netdevice () failures properly
[ PATCH ] kprobes / ia64 : refuse inserting kprobe on slot 1
ks8851 : Fix request_irq / free_irq mismatch
f2fs : wake up all waiters in f2fs_submit_discard_endio
ibmvnic : Sanitize entire SCRQ buffer on reset
usb : hub : Don ' t wait for connect state at resume for powered - off ports
fcoe : make sure fcoe frames are unshared prior to manipulating them
[ ALSA ] Add missing compat ioctls for ALSA control API
ath6kl : Check sme state before delivering disconnect event to cfg80211
sctp : Use correct sideffect command in duplicate cookie handling
memcg : fix leak of accounting at failure path of hugepage collapsing
[ PATCH ] Char : mxser_new , correct include file
xfs : fix off - by - one error in xfs_attr3_rmt_verify
md : use kzalloc () when bitmap is disabled
hwmon : ( stts751 ) buffer overrun on wrong chip configuration
rtc : limit frequency
HSI : nokia - modem : Use devm_kcalloc () in nokia_modem_gpio_probe ()
ip_gre : Set needed_headroom dynamically again
isdn / gigaset : adjust usb_gigaset tty write buffer limit
rt2x00 : Use IEEE80211_TX_CTL_STBC flag
pNFS : Ensure we check layout validity before marking it for return
drm : make drm_minors_cleanup () static
drm / mgag200 : check alloc_apertures () success in mga_vram_init ()
ipv4 : fib : Export free_fib_info ()
ASoC : rt5514 : Fix uninitialized calibration value
Input : powermate - fix oops with malicious USB descriptors
KVM : MMU : Set shadow pte atomically in mmu_pte_write_zap_pte ()
USB : serial : visor : fix crash on detecting device without write_urbs
xhci : Set URB actual length for stopped control transfers
mwifiex : fix command 0x2c timeout during p2p_find or p2p_connect
drm / nouveau / i2c / gp104 : initial support
aacraid : prevent invalid pointer dereference
ASoC : simple - card : Add a NULL pointer check in asoc_simple_card_dai_link_of
ASoC : simple - card : tidyup return method from probe ()
mfd : tc6393xb : Handle return value of clk_prepare_enable
KVM : VMX : fix page leak in hardware_setup ()
Btrfs : free and unlock our path before btrfs_free_and_pin_reserved_extent ()
hwmon : ( pc87360 ) Fix vrm write operation
staging , vt6656 / wpactl . c : Fix mem leak in wpa_ioctl ()
drm / mgag200 : free memory allocated with alloc_apertures ()
ipv4 : Missing sk_nulls_node_init () in ping_unhash ().
tracing : Add entry -> next_cpu to trace_ctxwake_bin ()
Btrfs : fix an oops when deleting snapshots
ath10k : abort scan properly if wmi_scan_stop fails
sctp : Correctly track if AUTH has been bundled .
xfs : fix kernel memory exposure problems
cris : add missing declaration of kgdb_init () and breakpoint ()
bpf : fix liveness propagation to parent in spilled stack slots
fm10k : rename mbx_tx_oversized statistic to mbx_tx_dropped
cred : copy_process () should clear child -> replacement_session_keyring
fix in thread . call
exec : -- cap do not set inheritable capabilities
Enforce a limit on value bytes size
Added stack overflow check for hermes :: vm :: hermesBuiltinApply
mm : fix prctl_set_vma_anon_name
zero the arep structure ( Coverity # 115822 )
shaper : fix reallocation
Bug : Lua can generate wrong code when _ENV is < const >
cJSON : fix memory leak on object parsing error
Verify that the native handle was created
OMXCodec : check IMemory :: pointer () before using allocation
Prevent a CHECK - fail due to empty tensor input in ` map_stage_op . cc `
Prevent one more div by 0 in TFLite
Prevent a division by 0
ksmbd : check nt_len to be at least CIFS_ENCPWD_SIZE in ksmbd_decode_ntlmssp_auth_blob
store mode : check for truncation
return early from ProcessRawBytesWithSeperators () when data length is 0
omnibox : Remove unregistered NOTIFICATION_BOOKMARK_MODEL_LOADED from action_box test .
Check the content setting type is valid .
[ pdf ] Defer page unloading in JS callback .
Always call UpdateCompositedScrollOffset , not just for the root layer
