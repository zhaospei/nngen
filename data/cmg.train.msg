siddec : reset bytes counter
mpeg2dec : Revert ranking switch with ffdec_mpeg2video . Fixes # 574461
mpeg2dec : Fix buffer memory leak in error code path
mpeg2dec : Hold ref to buffer while mapped
realmedia : Fix Memory leak in error case
rdtdepay : make buffer metadata writable before setting caps on it
diff : avoid potential null pointer dereference on error
df : Skip final '/' character when calculating basename .
daemon : parted : Don ' t crash if part - get - name called on non - partitioned device ( RHBZ # 1142158 ).
ovs_events plugin : Replace strncpy () with sstrncpy ().
modbus plugin : fix 2 leaks on error paths
bind plugin : plug small leak
perl plugin : Warn if " EnableDebugger " has been used after " LoadPlugin ".
src / utils_avltree . c : Add sanity check to c_avl_destroy ().
Fixed memory leak in collectdclient library .
teamspeak2 : malloc + memset -> calloc
Turbostat : Use the ' count ' gauge to count SMIs
Added the instance name to varnish API
virt plugin : compare pointer to NULL , not 0
write_http plugin : Add assertion for scan - build .
postgresql plugin : Disable queries that do not include an SQL query string .
Print a " starting ..." message on stderr if running in nofork mode , for
fix leak in cc_mslsa . c
strdup ( 0 ) now returns 0
kadm5_setkey_principal_3 not copying key_data_ver and key_data_kvno
Reposition a trace call which was dereferencing freed memory after
src : remove cpplint error using NOLINT directive
src : fix nullptr dereference for signal during startup
src : fix use of uninitialized variable
src : make CipherBase :: kind_ const
crypto : fix memory leak in randomBytes () error path
src : fix minor memleak in preload - modules
crypto : fix memory leak in CipherBase :: Final
bb # 11514 - check packSizes prior to dereference
return with CL_EFORMAT instead of CL_EDSIG
cid # 10403 follow - up
fix dumping of javascript in pdf
fixed CID 11651
fix CID # 10540
cid 12207 - fix error state for allocating altstr container
fix previous commit
show errno reason in freshclam on failure ( bb # 923 )
fix NULL dereference when running with a limited db
cid 12123 - fix error state in allocating memory in pdf finalize
High : cib : Fix the feature set check
Dev : crmd : fix typo
Low : execd : avoid memory leak when testing remote key
PE : Remove erroneous if - clause
TE : Detect changes to attributes of the cib object
Low : tools : crm_mon - resolve coverity UNINIT error
High : cib : Fix huge memory leak affecting heartbeat - based clusters
Low : controller : filter certain synthesized failures from CIB
decrease the repeating of the same log
High : cib : Correctly free memory when writing out the cib to disk
High : PE : Fix memory leak on processing message ( bnc # 780224 )
cib : Memory leak - Free query matches in update_attr ()
Dev : Tools : attrd - Fix value ++ expansion
High : PE : Parse fail - count correctly ( it may be set to ININFITY )
gui : Show messages as text not html
repository : plug memory leak
odb : mempack : fix leaking objects when freeing mempacks
fileops : do not overwrite correct error message on mmap
config : set an error message when asked to delete a non - existent key
remote : don ' t try to create tag annotations as refs / tags / v0 . 1 . 0 ^{}
tree : Check for `. git ` with case insensitivy
path : remove unused local variable
checkout : free last created directory
config : also free the XDG buffer
revwalk : remove tautologic condition for hiding a commit
revwalk : clear first - parent flag on reset
streams : openssl : Handle error in SSL_CTX_new
pool : Take into account malloc overhead & pool page size
refs : fix a memory leak
repository_head_unborn : clear error when HEAD is unborn
don ' t double free pkt
add extra check
Fix a double free on error recovering
make_absolute_path : return the input path if it points to our buffer
Axe the last ent
refs . c : abort ref search if ref array is empty
blame : don ' t overflow time buffer
block - sha1 : make the size member first in the context struct
make -- find - copies - harder imply - C
pack - objects : fix early eviction for max depth delta objects
git - pickaxe : fix nth_line ()
fix pack - object buffer size
range - diff : right - trim commit messages
http . c : Remove bad free of static block
git - add : warn when adding an ignored file with an explicit request .
git - log : do not output excess blank line between commits
rev - list -- header : output format fix
setup . c : verify_non_filename (): don ' t die unnecessarily while disambiguating
git - compat - util : make UNLEAK less error - prone
xmkstemp (): avoid showing truncated template more carefully
setup : make sure git dir path is in a permanent buffer
builtin - commit : fix summary output .
[ PATCH ] Fix git - init - db creating crap directories .
[ PATCH ] HTTP partial transfer support fix .
imap search : Fixed YOUNGER / OLDER
doveadm user : Avoid potential crashes when running via doveadm - server
fts : fts_expunge_log_uid_count () returned garbage on nonexistent log .
imapc : Fix memory leak in disconnected state
auth : Fixed non - auth passdb lookup when password had ".< encoding >" suffix .
lib - fs : fs_write_stream_finish ( file , NULL ) is now alias to fs_write_stream_finish_async ( file )
lib - smtp : server : Fix assertion failure occurring for a second failed BDAT / BURL command .
imapc : Don ' t send UID FETCH 1 :* after SELECT if mailbox is empty
lib : hash - format - fix leak on _init () failure path
lib - fs : ostream - metawrap shouldn ' t count metadata header as part of the stream offsets .
dbox rebuild : If alt directory doesn ' t exist , don ' t fail .
lib - storage : Fix accessing the same " raw " mailbox multiple times in process .
lib - smtp : smtp_server_connection_pending_command_data () - don ' t crash if smtp_parser is NULL
fs layout : Absolute paths as mailbox names shouldn ' t return index with INDEX = MEMORY
pop3 - migration : Don ' t even try to match message sizes with pop3_migration_skip_size_check = yes
lib - index : Fixed sequence lookup of newly created mails in transaction view .
i_stream_stat () should return NULL for closed istreams
data stack : Fixes to handling out - of - memory situations .
fts - solr : Removed deprecated waitFlush parameter from commit .
rfc822_parse_phrase (): Don ' t read outside data boundaries if input is empty .
doveadm backup : Don ' t unnecessarily delete mailbox when handling expunges
Missing error checking in epoll_create (). Patch by Andrey Panin
snarf : Make sure destination mailbox is open when syncing it .
Die if ( Solaris ) LDAP library returns wrong file descriptor .
auth : oauth2 - introspection lookup error should be internal failure
anvil : Penalty tracking was moving last checksums wrongly with memcpy ().
lib - ssl - iostream : Fixed a memory leak when verifying certificate name .
imap : APPEND crashed if invalid keyword was given as parameter .
lib - storage : Fixed assert - crash caused by previous stats change .
lib - storage : Previous change incorrectly reverted setting parent shared namespace usable .
sdbox : In - memory mailbox GUID differed from on - index GUID after mailbox_create ().
imapc : Added assert to make sure callback is never NULL .
istream - decrypt : Correctly check the header length for v1
lib - auth : Don ' t crash in auth_master_ *() lookups if caller hasn ' t created ioloop .
i_getpwnam (): Ignore EINVAL errors silently .
i_stream_read (): Added assert to make sure EOF isn ' t returned too early .
fts : Fixed handling NOT when using lib - fts
lib - storage : Fixed hooks with plugins that didn ' t override any vfuncs
fts : Fixed crash with some search queries in empty mailbox .
lib - index : Minor fix to handling atomic inc with INT_MIN value .
lib : io_loop_set_current () - do nothing if ioloop doesn ' t change
ssl - iostream : ssl_iostream_destroy ( NULL ) should be a no - op
lib - storage : If chdir ( home ) fails due to EACCES , don ' t log two errors .
push - notification : Fix mailbox unsubscribe event
placeholder -> noselect change broke LIST "" " foo /%" not showing foo /.
quota : Mark the mailbox deleted before starting to reduce quota .
libpassword : Deinitialize hash iterator in password_scheme_detect
lib - http : 32bit system fix .
acl : Crashfix for handling namespaces that don ' t have a paths .
imapc : Avoid duplicate FETCH BODY . PEEK [] when mail is already expunged
Removed accidentally committed debug warning .
reset modifylog sync mark at the end of opening index file
imap IDLE : Add also 172 . 168 / 12 to private networks .
authd : always use an empty buffer to read ident reply
dns : make function used only within this unit static
conf : Remove dead store .
libratbox / openssl : Fix possible memory leak with SSL certificate fingerprints
openssl : Fixed Curl_ossl_cert_status_request () not returning FALSE
ldap : silence clang warning
file_range : avoid integer overflow when figuring out byte range
imap : Added check for new internal imap response code
openssl : fix boringssl build again
hostip . c : return immediately from Curl_resolv_timeout () upon expired timeout .
smb : Don ' t try to connect with empty credentials
cookie : fix memory leak on oversized rejection
typecheck - gcc : allow CURLOPT_STDERR to be NULL too
Fixed another OOM problem , this time with test 64 .
mime : properly unbind mime structure in curl_mime_free ().
read_tcp_data () fix to get the proper buffer pointer and size
curl_ntlm_msgs . c : revert commit 463082bea4
sasl_sspi : Removed note about the NTLM functions being a wrapper
config - amigaos . h : ( embarrassed ) made the line shorter
sftp upload : expire to advance state machine
geez , I ' d ruined Nico ' s socklen_t define , corrected it now
fix compiler warning
Curl_pipeline_server_blacklisted : handle a NULL server name
get the display host name properly
avoid redundant work when reusing same connection
mbedtls : fix memory leak in error path
clarified the URL part based on the problems Martin Hilpert had
winbind : Fixed ntlm_auth expecting eol following commit e17c1b25bc33eb
urlglob : avoid NULL pointer dereference
Bug 592484 - invalid write in HB
Fix a g ++ error with -- enable - shell - use - argv
Add a line missed in the merge to 4 . 2 - dev for TRQ - 2058 .
Improved a line of code checked in earlier .
Fix a unit test mistake .
Fixed unit test for mom_job_func
Fixed unit test for pbsd_main .
Fixed unit tests .
removed code to free trq_server_ip in trq_main . Not needed
Changed place = core = x to start placing cores
Fix an infinite loop ( patch from Martin Siegert ).
removed memory leak in job_clone ()
Made it so cores or threads can be allocated on
fixed to memory leaks for error cases in procs_requested .
Revert the previous change - this doesn ' t solve fedora ' s problem .
moved sublock inside master lock
Changed the pbs_tcp_timeout value when reading the protocol
Fix a coverity complaint .
fixed unit test
undo partial patch
pua : safety check on tuple_id len
pua_dialoginfo : do not free dlginfo_cell too early
modules / ims_qos : missing return statement
rtpproxy : use reallocxf () to handle realloc failure
topoh : free nbuf
dialog ( k ): safety check for lookup in dialog table at shutdown
mohqueue : fix pointer for RTP statistics error
kazoo : check amqp version
dmq : initialize peer_list to NULL by default
kex : check if uptime value was initialized
tm : Ensure all contact attributes are initialised to NULL when serial forking
db_postgres : don ' t free pg query structure in store result function
usrloc : re - init sruid struct for each child
modules / usrloc : init _ul_sruid also in mi_child_init
pv : rpc commands updated to use rpl_printf ()
mohqueue : remove call from queue if caller does not exist
acc ( k ): Replace call to ( deprecated ) atoi () function with
carrierroute : export MI commands
* Fixed test on From URI length . Thanks to Papadopoulos Georgios for
tmx : allocate space to store ending 0 for branch value
modules / lcr : print debug message in case from uri regex matching fails
Changed rs_cms_get_transform () to return NULL if CMS is disabled .
Fixed leak in batch_queue_save ().
Fixed camera WB for Nikon D7100 .
Added some sanity checking in gui_drawingarea_motion_callback ().
Made paste defualt to all settings .
Changed a malloc to g_malloc
freeing filetype_str when not needed anymore .
ao / wasapi : more consistent / reliable method of computing extra WAVEFORMATEXTENSIBLE size
stream_id == MAX_STREAMS is invalid , since counting starts with 0 .
core : Set mpctx -> chapters to NULL at uninit
vo_opengl : do not leak previous FBO when reallocating it
demux_lavf : mark ASS tracks as always UTF - 8
fastmemcpy path fixed
af_scaletempo : Fix crash in option parsing
vo_opengl : disable drm_egl autopickup
vo_opengl : don ' t upload potentially uninitialized memory to GL buffer
input : fix off - by - one error
dvd : add the last chapter
audio : fix small memory leak
vaapi : fix uninitialized value read
check sh_video before seeking ; closes cid 56
wayland : don ' t change set fullscreen twice
eq range mapping - 100 .. 100 => 0 .. 100
vo_opengl : reset surfaces on equalizer updates
add YAXPAX and YAPAXI exports to msvcr80
workaround ! exit if there ' s no video stream ( hopefully it ' s a proper fix )
vo_opengl : error out gracefully when trying to use FBOs without FBO API
ffmjpeg yv12 422 fixed
demux : check whether stream is selected in demux_get_next_pts ()
gl_video : fix refcounting
ao_null : disable latency emulation
ao_wasapi : don ' t treat SetDisplayName () failure as fatal
fix memleaks ; patch by andrew calkin from gmail com
Added the connect2Server prototype .
av_log : map libav * debug messages to MSGL_DBG2
vo_direct3d : allow resizing before video init
vo_rpi : fix blackscreen before the first subtitle / OSD is rendered
af : avoid rebuilding filter chain in another minor case
demux_libass : actually set priv context
ehh . frame skipping fixed for - ovc frameno
video_out : free memory if initializing a VO fails
mplayer : don ' t show bogus audio PTS during hr - seek
sanity check : since chapter is 0 - based it can ' t exceed nr_of_ptts - 1
vo_vaapi : fix reference counting when using software decoding
ao_coreaudio_utils : compare full AudioStreamBasicDescription
Bugfix : ztex : Free bitstream in memory when done with it
Bugfix : lowl - ftdi : ft232r_close : Free device handle
Add the cgpu_info structure before avalon reset .
NULLing handle on destroy
Update the hashmeter at most 5 times per second .
Bugfix : setup_stratum_curl : Hold stratum lock until connection completes , to avoid potential races
Increase the speed of hashrate adaptation .
Bugfix : Clear total_bad_nonces when zeroing statistics
Bugfix : free adhoc string elist element when removing it from list
Remove off by one error .
Bugfix : Pass newlines as ASCII ( fixes lack of them in non - unicode builds )
Bugfix : knc : Perform le32toh on unknown - work nonces
ACPICA : test timeout on global lock interface
acpi : acpica : fix acpi operand cache leak in nseval . c
Added alignment and math macros
Tables : Fix spelling mistake in comment
aapts : Update for ACPICA interface change .
ACPICA : Fix for Linux source translation
AcpiDump : Allow tables that contain only an ACPI table header .
automated code cleanup
Added AML_NAMEPATH_OP to AmlDumpOperand case statement .
iASL : Update for ToPLD macro .
Modified various APIs to match our intent .
slirp : Fill TCP / IP header template after SYN reception
scsi : Make device scsi - disk reject / dev / sg *
target - cris : Build fix for debug output
usb : queue can have async packets
xilinx_spips : Fix bus setup conditional check
block : Replace u_int8_t , u_int16_t , u_int32_t , u_int64_t by standard int types
host - libusb : Correct test for USB packet state
Initialize a variable in all cases
aio : test node -> deleted before calling io_flush
hw / smc91c111 : Add explicit ' return ' rather than relying on fallthrough
vnc : Fix tight_detect_smooth_image () for lossless case
pseries : Don ' t allow TCE ( iommu ) tables to be registered with duplicate LIOBNs
posix - aio - compat : Plug memory leak on paio_init () error path
pci : memory leak of PCIDevice :: rom_file
ahci : fix buffer overrun on invalid state load
petalogix_ml605_mmu : Attach ethernet to machine
hda - audio : fix non - mixer codecs
s390 - ccw . img : Fix sporadic reboot hangs : Initialize next_idx
vl : free err
block / vmdk : do not report file offset for compressed extents
rtl8139 : skip offload on short Ethernet / IP header ( CVE - 2015 - 5165 )
ide : Add forgotten VMSTATE_END_OF_LIST in subsection
lm4549 : Fix buffer overflow
virtio : Plug memleak by freeing vdev
XBZRLE cache size should not be larger than guest memory size
mips : missing syscall returns wrong errno
target - tricore : Fix LOOP using wrong register for compare
sheepdog : fix NULL dereference in sd_create
cirrus_vga : remove unneeded reset
sheepdog : fix snapshot tag initialization
kvm : Drop redundant kvm_enabled from cpu_thread_is_idle
block : Add SG_IO device check in refresh_total_sectors ()
ccid - card - emul : do not crash if backend is not provided
qcow2 : Free allocated L2 cluster on error
qemu / iovec : Don ' t assert if sbytes is zero
vnc : Don ' t assert if opening unix socket fails
tcg - i386 : Allocate call - saved registers first .
qemu - char : report udp backend errors
vt82c686 : avoid out - of - bounds read
sparc / leon3 . c : fix memory leak
block : Catch ! bs -> drv in bdrv_check ()
virtio : avoid buffer overrun on incoming migration
vhost - user : verify that number of queues is non - zero
target - arm : Fix TCG temporary leaks for scalar VMULL
qcow2 : Reject too large header extensions
target - s390x : Add missing tcg_temp_free_i64 () in disas_s390_insn (), opc == 0x8e
virtio - 9p : Check the return value of llistxattr .
i8259 : Do not update IRQ output after spurious pic_poll_read
QemuOpts : check NULL input for qemu_opts_del
exec : always use MADV_DONTFORK
xhci : add xhci_cap_write
fw_cfg : add required header files
vfio - pci : Fix MSI / X debug code
xen_disk : mark ioreq as mapped before unmapping in error case
sheepdog : fix compile error on systems without TCP_CORK
ehci : schedule async bh on async packet completion
rtl8139 : skip offload on short TCP header ( CVE - 2015 - 5165 )
hw / intc / arm_gic : Fix NVIC assertion failure
target - mips : log instructions start in TCG code
tcg - s390 : Icache flush is a no - op .
x86 : fix miss merge
vmdk : Fix format specific information ( create type ) for streamOptimized
arm : fix compile on bigendian host
sysbus : make SysBusDeviceClass :: init optional
spapr : Provide an error message when migration fails due to htab_shift mismatch
bb # 11514 - change cli_malloc with cli_calloc
handle CL_ETIMEOUT in cl_strerror
dconf : disable swizzor heuristic by default
don ' t cache possible unwanted
bz10893 : fixes bug for hash calculations when using -- allmatch .
bb # 11370 - do not update mirrors on custom database urls
hwp3 . x : enable password cancelling of parsing
CID 12072 - added mssing return
Deal with similar wrap around in literal_len in the src buffer
Related : rhbz # 1602527 add comment to silence leaked_storage for ' glyphs '
libsmartcols : fix stupid typo
partx : work properly with 512 sectors ( dos PT )
sulogin : use EXIT_ *
utmpdump : make analyzers happy
fdisk : non - linux support ( MAXPATHLEN )
lscpu : avoid double free
fdisk : add support for the Xtensa architecture
libblkid / minix : Match minix superblock types
blkid : fix typo in ID_FS_AMBIVALEN =
mount : remove useless if - before - free tests
setpwnam : remove non - ANSI definitions [ smatch scan ]
lib : [ procutils . c ] improve robustness
libblkid : don ' t gather PART_ENTRY_ * values for binary interface
tailf : fix memmove () usage
rev : Avoid calling free in a signal handler
sulogin : don ' t run if not found passww entry [ coverity scan ]
login - utils : remove unwanted newlines from wall
libblkid : don ' t use fstat () in blkid_get_dev_size ()
Unload the GeoIP db when disabled .
Fix a memleak in torrentmodel .
Fixed typo reported at github # 1552
Settings : add missing save call for the ' shortcut / gkey ' setting .
fix : segfault through python bindings with fd to non - package (# 24089 ).
accept NULL as a valid prefix in rpmdbOpen ()
Fix a small memleak in rpmsign tool
fix parsing of empty doublequoted strings in spec file ( rhbz # 456103 )
don ' t close std [ err , out ] after a verify script
added an error message when rmdir () fails
fixed a memory leak
don ' t let hdrs [] be sparse
Log an error on attempt to sign V3 packages ( RhBug : 517818 & others )
don ' t assume write ( fd , buf , 0 ) always returns w / o error
bc_eval : legacy scripts may not contain some string list options
conversations : don ' t rename to self !
Have to make unspecified - domain be in writable storage .
libconfig : more universal fix for logging fopen () error
Oops , got the sense of a return backwards .
jmap : free memory of ical component clone
http_caldav . c : dont leak userid in caldav_delete_cal ()
jmap : getMailboxes : fix sortOrder memory leak
mboxlist : don ' t crash without patterns
strip batchsize on full sync
lmtpd . c : don ' t try local delivery if mlookup () fails
snippets needs a mailbox at the moment too
mboxname : free recipient in cleanup
lmtpd : use xmalloc
buf : don ' t realloc for every buf_ensure
mailbox : cleanup correctly when DELETED records retained
http_caldav_sched : handle blank attendees
cache : if fd has gone away , make sure mmap is clean too
search : always index the target of a rename
prot : disable boundaries for now
http_caldav . c : don ' t allow an attendee to reply to a non - existent invitation
ctl_zoneinfo . c : skip any files that we can ' t parse as iCalendar
mboxlist : don ' t fail on subscribed but non - existent folder
http_dav . c : reset txn -> buf in MKCOL after we ' re done creating XML response
dav : zero out index_state
resolved a problem with delayed loading of keys
Modules : fix client blocking calls access to invalid struct field .
Cast sentlen to int before comparison wit bufpos .
cli : Only print elapsed time on OUTPUT_STANDARD
fix to latency monitor reporting wrong max latency
Streams : return an error message if using xreadgroup with '$' ID .
fix recursion typo in zmalloc_usable
Scripting : Reset Lua fake client reply_bytes after command execution .
Fixed possible buffer overflow bug if RDB file is corrupted .
rtmpproto : Validate the embedded flv packet size before copying
avcodec / fic : clear slice_data
avfiltergraph : set deprecated filter_count .
lavc : set avctx -> hwaccel before init
mp3adu : Set the channel layout properly
remove unnecessary - 1 from snprintf len
udp : Fix local_port management
indeo4 : fix out - of - bounds function call .
avio : Free URLContext private data allocated via AVOptions
avprobe : free options at the end of main ()
lavf : fix the comparison in an overflow check
workaround apps which dont provided correctly padded streams
avplay : Check frame allocation inside video_thread ()
movdec : Avoid av_malloc ( 0 ) in stss
movenc : Free the buffer returned by url_close_dyn_buffer , regardless of the size
avplay : free the active audio packet at exit .
mpegtsenc : use correct PES stream_id for AAC
h264 : Use the correct argument context in debug log
mov : Add support for zero - sized stsc runs .
dca : Validate the channel map
avconv : fix off by one check in complex_filter
oops , potential overflow on really large blocks
tta : fix off be 1 error in the end detection .
sipr : Use memmove () to copy overlapped buffers .
flvdec : Check for overflow before allocating arrays
Check that the seekheads were created
qsvenc_hevc : use the correct HW plugin UID
adpcmenc : Do not set coded_frame -> key_frame .
Pad the extradata in the AAC ADTS to AudioSpecificConfig bitstream filter .
flacdec : set default sample_fmt before parsing extradata
tls : fix compilation when both gnutls and openssl are enabled
force sample for qcelp when not stored in mov , fix # 968
fixing interlaced dct with 4mv
fix size of header data
remove unnecessary shift
rtpdec_vp9 : Make sure to free the temp buffer on close
dfa : check for invalid access in decode_wdlt ().
vc1dec : Override invalid macroblock quantizer
rv34 : Avoid NULL dereference on corrupted bitstream
png : Support RGBA64 pixel format
vf_gradfun : fix uninitialized variable use
prores : Evaluate all the quantizers
id3v2 : add a mimetype for bmp pictures .
anm : prevent infinite loop
adts : Fix PCE copying .
Correct the threshold of get_ur_golomb_jpegls () at which the optimized
xxan : don ' t read before start of buffer in av_memcpy_backptr ().
a64 : check that extradata exists before reading from it
wmapro : return early on unsupported condition
proresdec : Properly make sure an index doesn ' t run past the limit
mpegmux : add stuffing to avoid incomplete PCM frames
rtpenc_chain : Use the original AVFormatContext for getting payload type
avio : Apply avoptions on the URLContext itself as well
avprobe : Zero the allocated avio buffer memory
h264 - mt : fix deadlock in packets with multiple slices ( e . g . MP4 ).
tinfoil patch : accept no widths that are no multiples of 4
clear the whole vert_pred buffer for 24 bit decoding .
mov : fix stream extradata_size allocation
avcodec : set avpkt -> size to 0 if encode2 () did not output a packet
sol : return error if av_get_packet () fails .
reset interlaced_dct flag
fixing MVs in hq mode
return eof / error if only ffm header has been written , should fix # 815
mpegts : Silence " can ' t seek " warning on unseekable
vaapi_h264 : Add missing return value check
used defined name for testing error resilience level
vc1 : Do not return an error when skipping b frames
oma : check avio_read () return value
Removes misleading const qualifier , gets rid of two compiler warnings
mxfdec : return error if no segments are available in mxf_get_sorted_table_segments
cbs_h2645 : Return error if writing fails
simplify format string for writing pix_fmt string .
http : Always allow no - op seek
avidec : fix a memleak in the dv init code .
fix a memleak in av_metadata_set ()
hlsenc : use the basename to generate the list entries
Flush the remaning chars in the input buffer after reading the
disconnect client if trailer is sent
cbs_h2645 : Fix memory leak on when reading SEI fails
issue 10 - zmq_strerror problem on Windows
add a memory check just in case
Quench reject_delay to be no less than zero
made paircompare () static
Initialize all of the structure
Added support for password - protected Jabber chatrooms .
Fixed md -> grouplist memory leak .
imcb_file_send_start : handle ft attempts from non - existing users
Improved failure handling in jabber_si_handle_request (). If the imcb
Catch an infinite loop in smpp_handle_dlist (). Fixes bug 246 .
Call the " without FCS " version of the Ethernet dissector .
Reset the captured - packet count when we start or restart a capture .
GTK : Bugfix not allowing extcap capture filters to be verified .
RTPS : fix Result of operation is garbage or undefined
Avoid a possible dereference of null pointer .
wlan_stat : fix a memory leak
From David Carne :
Gryphon : do not use col_set_str with non constant strings
gcc - 4 . 1 . 1 thinks ' retval ' might not get set inside the TRY (). Initialize it .
dirty hack to make things compile without libgcrypt
Qt : fix use of uninitialized variable
frame_data : Reset subnum if visited is resetted
avoid a crash when tree is null
tvbuff : fix compilation with gcc - 4 .
GTPv2 : fix Copy - paste error ( CID 1158587 )
decode as : Fix small memory leak
sprintf big fix
Initialize all tvb values in FieldInfo_get_range .
gtk : Fix null dereference in stats
DCT2000 : prevent a stack buffer overflow
packet - sip . c : 1469 : warning : unused variable ' contact_param_end_offset '
DICOM : Revert 3752a643ac5c85622822c88c69d83ce23828acb8
ssl_association_remove (): free assoc -> info too
gtk : fix memory leak in profile dialog .
dot11decrypt : free memory on exit ( found by clang ).
Qt : Add a bottom border to the packet map .
plugin_if : fix no previous prototype for ‘ ext_toolbar_find_item ’ [- Wmissing - prototypes ]
bgp : add return check ( CID 1354417 ).
Extract the center frequency from Peek tagged files .
Stuff the " Don ' t show this message again " checkbox in the dialog ' s
HTTP2 : Fix double free
Qt : add initializers ( CID 1159363 ).
extcap : add more checks for extcap disabled .
From : Susanne Edlund
ws_pipe : Zero an array .
qnet6 : don ' t calculate crc if length is negative
Qt : Apply recent column widths when columns changed
extcap : fix a compile error with Qt4
get_persconffile_path () return malloc ' d memory ,
RTP : Parse problem in dissect_rtp_hext_rfc5215_onebyte
Add a cast to try and fix the OSX buildbot .
Qt : fix buffer alarm threshold parameter retrieval in multicast statistics dialog
RADIUS : Initialize vendor attribute type to zero
pcapng : Free option_content on error
Qt : add initializer ( CID 1230901 ).
Add another check to radiotap - iter initialization to catch another out - of - bounds
Fix a memory leak which occured upon registering an already existing
Fixed icorrect key length after updating constant index
fix double free
Fixed an iconv_strrpos () bug that the function doesn ' t return accurate
fix potential leak
fixed php version detection
Added check for calloc () failure
fix typo and bug # 41962
cannot import nodes without document
keep previous test to avoid any behavior change
Fixed incompatible pointer type
Fixing possible remote overflow due to mbstring translation .
Set the correct function state during execution . This is mainly to have
fix yet another possible segfault
Initialize all struct members : Necessary for reflection
fix yet another leak
fix memory leak
fix memory leak
replace llabs with imaxabs
fileinfo : fix misleading indentation in libmagic
Freeing the memory would be a good idea ...
Fixed heap overflow
Fixed a memory leak inside imap_utf8 ()
cleanup unused variable
Make it possible to disable post_max_size
oops . Copy - paste - magic . :)
Added missing charset .
Fixed assignment of reference to itself
set default response code to 200
Added check for small ZEMD_MM_SEG_SIZE
don ' t leak on error
Prevent opening of empty filename ""
Fixed bug codes in SplFileInfo :: getExtension
Added an E_NOTICE when precision value is truncated
isync with 5 branch
Fixed a possible memory leak on open_basedir validation
fix segfault when running run - tests . php , and general potential for corruption
efree match_sets on an error here - found by Coverity
Fixed memory leak
prevent flags overflows
efree match_sets on an error here - found by Coverity
Unterminated paren group in BETHREADS definition of tsrm_tls_set () macro
make ftp compile on unix again
support sucking data from streams in file_put_contents ()
Fixed possible memory leak .
Fixed uninilized intern
dir : Reserve the whole @@ prefix
run : Block setns ()
helper : Allow exec on the tmpfs .
update - appstream : Don ' t fail badly if remote has not appstream branch
flatpak remote - ls - u : only consider apps from the current remote
run : Don ' t allow chroot ()
flatpak_number_prompt : Add missing va_end
run : Don ' t allow unmounting filesystems
create - usb : Fix a leak of the list of extensions
xdp - fuse : Add parentheses to clarify precedence in a conditional
synIDattr (): true color awareness
vim - patch : 8 . 0 . 0584 : memory leak when executing quickfix tests
shada : Fix offset saving
viml / executor : Fix check - single - includes
Fixed a memory leak with alternative names
winpr - handle : fixed mem leak when closing named pipes
core : Added missing \ 0 termination to crypto_print_name
libfreerdp - gdi : fix gdi_Glyph_Free () segfault
core : free polyline and polygon_sc points
Fixing issue 245
Be on the safe side when freeing stuff
Parser : be more conservative when estimating the line size
getty - generator : fix segfault when $ container_ttys is not set
logind : when enumerating acl devices for seat0 , don ' t set a tag
fuzz - unit - file : allow a space between variable name and "=" (# 8670 )
systemctl : add missing OOM check
journalctl : properly detect empty journal files
sd - daemon : verify NOTIFY_SOCKET path length
journalctl : show fields requested with -- field in full
sysusers : fix memleak (# 4430 )
systemctl : fix detection of active units
shared : time - dst : Avoid buffer overflow
ude_rules . c : fix possible NULL pointer dereference in get_key ()
kdbus : make sure msg structure is properly initialized
[ PATCH ] udevd : fix valgrind warning
resolved : return exit code of event loop
tmpfiles : restore previous behavior for F / f
modules - load : filter out double modules
install : don ' t choke on dead links
locale : add printf attribute to log_xkb ()
systemctl : fix memory leak in systemctl_parse_argv
systemctl : fix check for template units
vconsole : copy font to 63 consoles instead of 15
journal - upload : allow the tool to start
sd - bus : let ' s use mfree () where we can
systemctl : do not print header if no units will be listed
vconsole : add copyright line
util : look at STDOUT , not STDIN , when determining tty columns
machinectl : don ' t show ". host " pseudo - machine in list by default
systemctl : allow listing cgroups of containers
resolved : use single message for both dbus and signal calls (# 3515 )
resolved : don ' t override zone item state after starting the probe
login : fix mem leak
util : ignore_file should not allow files ending with '~'
[ PATCH ] allow NAME_SIZE > SYSFS_PATH_MAX
journalctl : free arg_file on exit
shared : handle unnamed sockets in socket_address_equal ()
bus : properly unset default bus pointer when destroying last reference
locale : make sure that l is freed
acpi : make sure we never free an uninitialized pointer
base - filesystem : avoid all searching if the link already exists
swap : don ' t add too many deps for swap files form / proc / swap
build - sys : make gcc shut up
core / cgroup : first print then free
libudev : fix sysnum logic for digit - only device names
IMPORT : 2048 -> 4096 bytes buffer
logind : never select closing sessions for a VT
path - util : when parsing a timestamp we don ' t know the timezone
journalctl : don ' t choke on entries with no MESSAGE = field
json : avoid cleanup of unitialized variable
virt : make sure we don ' t ignore some errors
sd - bus : correct size calculation in DBus fd receive
logind : prefer new sessions over older ones on VT switches
path - lookup : use correct cleanup function for strv (# 5007 )
silence " comparison between signed and unsigned "
journald : do not free uninitialized pointer in error path
core / transaction : fix cycle break attempts outside transaction
Allow the creation of 48 bit primes
check that ctx is not a null ptr before deref cid # 131
heimdal : Fix the developer O3 build
Check if header is there
roken_detach_prep () close pipe
k_gethostname -> gethostname
catch error code
Prototypes for keyfile and unseal_key .
add -- no - embedded - cert and -- embed - leak - only
make compile again
improve msft compiler case
don ' t leak memory
make EVP_BytesToKey return an error
avoid double free , coverity # 153
kdc : validate sname in TGS - REQ
kadmin : del_enctype check for bogus keys
cryptostats cleanup from Dave Mills .
Fixed loop condition .
settings : GatewayBypassLocal TRUE by default
Fixed string size calculation in winpr_HexDump
Fixed NULL dereference .
Fixed RSA generation for OpenSSL >= 1 . 1
Fixed memory leak .
xfreerdp - client : xi tweak2
Fixed malloc with 0 size .
primitives : renamed NEON detection function
core : free polyline and polygon_sc points
Added assertions to detect invalid call sequences of < transport_free >
Fixed null pointer dereference .
Fixed memory leak .
libfreerdp - core : fix server - side transport_attach crash
core : reset crypto key counters when the keys are initialized
Fixed # 6009 : Bounds checks in autodetect_recv_bandwidth_measure_results
Fixed argument check in < tls_disconnect >
libfreerdp - utils / args : fix wrong argument index for plugin name .
Fixed # 6005 : Bounds checks in update_read_bitmap_data
Fixed oob read in ntlm_read_NegotiateMessage
Fixed GHSL - 2020 - 101 missing NULL check
rdpsnd : follow rdpdr and free the stream from svc_plugin
libwinpr : check return value of realloc .
Fixed # 6011 : Bounds check in rdp_read_font_capability_set
winpr / file : add missing NULL check
Fixed missing variable type
Deactivated glyph cache .
libfreerdp - rfx : encoder must increase frameIdx or mstsc would crash .
libfreerdp / core : transport_write unchecked parameters
Fixed leak of client random .
flooddetach : add nick changes - close # 941
gdImageScaleTwoPass memory leak fix
inclode strerror in recv error message MINOR
remove unit test spam from mongos
fix authorization_manager_test build
increase timeout on killing
fix BSONObj ownership
assert that you can ' t shard config db
Protect from missing CodeGen .
Init member ( thanks , valgrind ).
Document a parameter .
setForcedLangOptions -> adjust .
fix warning on MacOS X .
std :: vector :: data () is not standartized and on windows it cause build failure .
dv : zero dsp before init , this fixes use of uninitialized dct_bits
wavpack : Fix an integer overflow
avcodec / utils : Better check for channels in av_get_audio_frame_duration ()
af_aresample : check ff_get_audio_buffer () failure
dxva : DXVA2_ModeHEVC_VLD_Main10 does not support Main
fix segfault with lol - ffplay2 . mpg ( dunno if this is exploitable , probably not easily )
h264_cavlc : check the size of the intra PCM data .
vf_hqdn3d : fix uninitialized variable use
cdgraphics : initialize buffer
avcodec / mimic : Fix runtime error : left shift of negative value - 1
ffplay : fix type of time_diff in waveform display
avformat / utils : fix calculating the absolute difference of timestamps
avcodec / vorbis : 1 << 31 > int32_t :: max (), so use 1u << 31 instead .
avcodec / dcadec : add an assert to silence a uninitialized variable warning
avcodec / pthread_frame : Fix memleak of AVCodecContext on error
avformat / yuv4mpegdec : fix " warning : field_order may be used uninitialized in this function "
avcodec / mpegvideo_enc : Fix possibly exploitable security issue with - bf
jv demuxer : prevent video packet size overflow
check qblur > 1 ( prevent assert failure / segfault )
r3d : fix an invalid read introduced in 6bf4c1d
avformat / avidec : initialize header array
mvdec : check var_read_string () return value
fix crash on non - AltiVec powered machines : MPV_common_init_altivec doesn ' t check mm_flags
lavu / opt : dont crash on freeing NULL AVOptionRanges
brstm : fix use of uninitialized variables
avformat : unref packet after storing it in internal packet queue
hevc_cabac : decrease CABAC_MAX_BIN
arm : fix use of uninitialised value in ff_fft_fixed_init_arm ()
pthread_frame : Unreference hw_frames_ctx on per - thread codec contexts
avformat / nutdec : fix packet end clearing
Make the RTSP muxer use rtsp_hd_out , for consistency
oggparsevorbis : Remove code messing with cur_dts .
lavu / opt : validate range before dereference
ffprobe : fix use of uninitialized variable
avcodec / lcldec : initialize encoded correctly
wmavoice : Check for out of bound writes
pngdec : check for bprint finalization sucess on icc data parsing
avcodec / hevc : clear filter_slice_edges () on allocation
wmalosslessdec : make MCLMS arrays big enough for what is written into them .
avcodec / binkaudio : clear padding area of packet_buffer
avcodec / g2meet : Also clear tile dimensions on header_fail
lpc : Remove false positive compilation warning about weight being
tests / checkasm / checkasm : Give macro a body to avoid potential unexpected syntax issues
avformat / vfq : fix deadlock in vqf_read_header ()
Loop up to MAX_THREADS instead of h -> s . avctx -> thread_count to free the thread
svq3 : propagate codec memory allocation failure in context init
avcodec / movtextdec : run mov_text_cleanup () before overwriting pointers
ff_h264_direct_ref_list_init : fix B slice check .
avcodec / lagarith : Fix runtime error : left shift of negative value - 1
lavfi : set sample_rate in the default allocator .
url_alloc_for_protocol : fix use of uninitialized variable
avformat / rmdec : zero string destination before use
avcodec : fix uninitialized variable read
avformat / mov : fix integer overflow
snowenc : dont crash with gray but exit with an error msg .
v410dec : Check for sufficient input data . Fixes crash
bitstream : build_table , check table_nb_bits .
shorten : fix " off by padding " bug
truemotion1 : fix leaking frame on init failure
avformat / mpegts : check avio_tell () return code before using it
avcodec / aacdec_fixed : fix : left shift of negative value - 1
v410dec : Check for sufficient input data .
vf_lut : make config_props work properly when called multiple times .
avformat / matroskadec : check generic audio deinterleaver sub_packet_size against frame_size
matroskadec : fix integer overflow
avcodec / ivi : Free custom blk_vlc
ffprobe : check av_frame_alloc () failure .
j2kdec : make curtilenum integer .
jpeglsenc : Mark codec as init - thread - safe and init - cleanup
indeo4 : Fix global array overread .
avsdemux : check for corrupted data
gifdec : gif_copy_img_rect : Fix end pointer
avformat / utils : avoid overflow in update_stream_timings () with huge durations
avcodec / sanm : Reset sizes in destroy_buffers ()
avformat / asfdec_o : Check size_bmp more fully
h264 : don ' t initialize missing pictures when using VDPAU .
avsdemux : check for out of bound writes
pcx : properly pad the scanline
mpeg4videodec : disable frame multithreading for GMC , its not implemented at all
avcodec / flacdec : Fix signed integer overflow in decode_subframe_fixed ()
cmdutils : use sws_freeContext () instead of av_freep ().
pass strict_std_compliance to audio context too
h264 : unref leaking error recovery picture
fraps : check for overread
webmdashenc : Fix potential memory leak
dca : Read params -> pancABIT0 [ param_index ] only if part0 is set
bessel : check that the function terminates as expected by av_assert2 ().
psymodel : dont apply lowpass filters with a cutoff close to the nyquist
libavformat / rtpdec_asf : zero initialize the AVIOContext struct
snowenc : check minimum resolution
avformat / http : Use av_freep () avoid leaving stale pointers in memory
avcodec / jpeg2000 : Fixes integer overflow in ff_jpeg2000_ceildivpow2 ()
flacdec : silence several " warning : X may be used uninitialized in this function "
lavfi / frei0r : fix crash when parameters are not set .
ac3_fixed : fix out - of - bound read
mpegvideo : fix null pointer dereference on switching streams
asplit : don ' t leak the input buffer .
libavcodec / exr : fix channel size calculation for uint32 channel
avutil / frame : fix crash with av_frame_unref ( NULL )
matroskadec : only return corrupt packets that actually contain data
bktr : fix warning : missing braces around initializer
ffmpeg : fix handling or empty audio packets
adpcmenc : check for coded_frame allocation failure
avformat / concatdec : Enable auto_convert by default
avidec / guess_ni_flag : fix harmless integer overflow
qt - faststart : Avoid leaking memory if encountering a file with double ftyp atoms
h264 : allow debuging SEI types
avformat / utils : check for overflow before reallocating side data
mpegvideo : clear overread in clear_context
s3tc : fix shift overflow by using unsigned constant
avcodec / imc : Fix bitstream buffer padding
samplefmt : avoid integer overflow in av_samples_get_buffer_size ()
avcodec / scpr : Check y in first line loop in decompress_i ()
swresample / resample : Limit filter length
mpeg12 : fixed parsing in some mpeg2 streams
adxdec : Validate channel count to fix a division by zero .
hls : fix integer overflow
avcodec / shorten : Sanity check maxnlpc
avcodec / hevc_sei : fix use of uninitialized variables in decode_nal_sei_frame_packing_arrangement ()
motionpixels / mp_decode_frame_helper : assert that the first pixel doesnt reuse the last .
avformat / mov : Check extradata before access
avcodec / wavpack : initialize pointers to silence warning about them possibly being uninitialized
libavformat / http : Fix memory leak in get_cookies .
avformat / avidec : Remove ancient assert
avcodec / tiff : Check for multiple geo key directories
wtvdec : fix integer overflow
doc / examples / decode_video : Fix format string vulnerability
doc / examples / http_multiclient : Fix resource leak
avcodec / utils : Fix overflow in get_bit_rates computations
h264 : silence warning about array index being out of bounds
lavf / tee : initialize ret in parse_bsfs ()
avcodec / interplayvideo : Check that enough data is available for opcode_0xA
asfdec : check stream_index for validity
id3v2 : prevent unsigned integer overflow in ff_id3v2_parse ()
indeo4 : check band -> scan
avformat / flac_picture : clear padding area
Fix a crash when probing img2 format with a NULL filename .
flvdec : check return value of create_stream ()
lavc / utils : fix extra ASS sanity check in convert_sub_to_old_ass_form ()
bmp : Fix warning X may be used uninitialized in this function
twinvq : validate that channels is not <= 0
eatqi : replace break by goto .
avcodec / dcadec : Check that the added xch channel isnt already there
hwcontext_d3d11va : fix crash on frames_init failure
mpeg4videodec : Fix " warning : dc_pred_dir may be used uninitialized in this function "
avcodec / mpeg4videodec : Check for bitstream overread in decode_vol_header ()
pcx : round up in bits -> bytes conversion in a buffer size check
sapenc : Free AVStream -> info on cleanup
avcodec / utils : check the private context class
avcodec / hevc : clear HEVClc when its deallocated in hevc_decode_free ()
Shut up an uninitialized variable GCC warning in VP8 .
avcodec / dvbsubdec : Check for duplicate regions in dvbsub_parse_page_segment ()
wma : Lower the maximum number of channels to 2
kill uninitialised variable warning in mp3_read_probe ()
aes : fix array index out of bounds warning
avcodec / pixlet : Fix reading invalid numbers of bits
vmd : use the PALETTE_COUNT constant uniformly
mjpegdec : Properly set the context colorspace info
fix infinite loop ( suggested change by rjayne at convera dot com )
avformat / rtspdec : Use av_mallocz_array ()
ffmpeg : check avpicture_get_size () retuen value
avcodec / mpegaudiodec_template : use av_freep (), do not leave stale pointers in memory
add check for invalid channel mode when coupling is used
dsicinav : Check for out of bounds reads
avformat / vqf : check a few more bits in probe
avformat_alloc_context : initialize raw_packet_buffer_remaining_size .
qdm2 : avoid integer overflow
wma : initialize prev_block_len_bits , next_block_len_bits , and block_len_bits .
asfdec : set nb_streams to 0 in the asf_read_close
avformat / http : return EOF from ff_http_do_new_request if previous response said Connection : close
drawtext : fix leak with timecodes
avcodec / g2meet : Fix order of align and pixel size multiplication .
avcodec / diracdec : Fix integer overflow in INTRA_DC_PRED ()
avformat / omadec : Use 64bit for ret to avoid overflow
avformat / movenc : Write version 2 of audio atom if channels is not known
tta : fix off be 1 error in the end detection .
avcodec / takdec : Fixes : integer overflow in AV_SAMPLE_FMT_U8P output
AMV : Fix possibly exploitable crash .
aviobuf : Handle a NULL buffer in avio_close_dyn_buf
jpeg2000 : Make nreslevel fields int
asfdec : prevent possible memory leak in the asf_read_metadata_obj
avcodec / mpeg12dec : Check for overread in mpeg_decode_slice ()
flvdec : Check for overflow before allocating arrays
avcodec / jpeglsdec : assert that overflows end at exactly x = w
h264 : don ' t sync pic_id between threads .
matroskadec : Ensure time_scale is nonzero , fixes divide - by - zero if the file
wmavoice : initialize best_hist_ptr to NULL to prevent ( incorrect ) warning .
avformat / avidec : Limit formats in gab2 to srt and ass / ssa
avformat / rmdec : Do not pass mime type in rm_read_multi () to ff_rm_read_mdpr_codecdata ()
avformat / format : Check for av_guess_format () failure
perform sanity check on sample rate in electronicarts demuxer
h264 : error out on unset current_picture_ptr for h -> current_slice > 0
avformat / movenc : Do not pass AVCodecParameters in avpriv_request_sample
jpeg2000dec : Check bpno in decode_cblk ()
opusdec : Don ' t run vector_fmul_scalar on zero length arrays
vaapi_h264 : Scale log2_max_pic_order_cnt_lsb with max_b_frames
fix vdpau vc1 interlace modes
lavd / alsa - audio - common : mark default device in device list
rtpdec_qdm2 : Don ' t try to parse data packet if no configuration is received
avcodec / get_bits : add av_assert2 to get_bits_long ()
avcodec / mpeg12dec : check block index in mpeg2_fast_decode_block_non_intra ()
rawdec : use av_shrink_packet .
avcodec / hevc_refs : fix potential use of uninitialized min_idx
flvdec : allocate large enough buffer so get_bits () doesnt overread
avcodec / truemotion2rt : Check input buffer size
amovie : avoid crash in amovie_request_frame () if no samplesref is available
pthread_frame : fix uninitialized variable read
Remove a NAL unit ' s trailing zero bytes even when dst_length is 1 .
Fix a crash caused by a null coded_picture pointer in ffserver .
wmaenc : require a large enough output buffer to prevent overwrites
avcodec / put_bits : Assert that size in set_put_bits_buffer_size () does not cause integer overflows
mpegpsenc : show first SCR / DTS at debug level
ape : Fix null ptr dereference with files missing a seekatable .
avformat / mpc8 : fix broken pointer math
ituh263 : reject b - frame with pp_time = 0
avcodec / ffv1dec : Check for 0 quant tables
avcodec / h264_refs : Do not set reference to things which dont exist
examples / vaapi_encode : Fix leak on hwframe init failure
avcodec / jpeg2000 : Check that codsty -> log2_prec_widths / heights has been initialized
lavc / mediacodec_wrapper : fix potential jni global reference leak
avutil / lzo : Fix integer overflow
utils : fix integer overflow with DURATION_MAX_READ_SIZE
audemux : Check channels isnt 0
aasc : check before reading the first 4 byte , fix overread
avcodec / mlpdec : Fix : runtime error : left shift of negative value - 8
avcodec / adpcmenc : fix integer overflow / undefined behavior in STORE_NODE ()
avformat_open_input (): Add braces to shut up gcc warning .
lzw : check for overread
swscale : fix another integer overflow at large dimensions / rescales .
lavfi / select : make sure avctx is set before closing it .
avformat / ipmovie : check OPCODE_INIT_VIDEO_BUFFERS size more completely
avformat / mov : Change the type of the r / g / b variables
Check for out of bound writes in the wmavoice decoder .
h264 : Fix memory leak on ff_h264_decode_init () failure
[ PATCH ] Fix crash when initializing multi - threaded decoding for corrupted file .
vc1dec : silence " may be used uninitialized in this function " warning from gcc
avformat / icodec : reduce score returned on probing
Fix an overflow in the predictor .
rv34 : fix signed multiplication overflow
golomb : fix end of bitstream check
tests / api / api - seek - test : Silence compiler warnings about uninitialized variables
avcodec / utils : Fix signed integer overflow in rc_initial_buffer_occupancy initialization
unset Track sets , relying in AVStream -> priv_data and will be freed in av_close_input_file
proresenc : Write the full value in one put_bits call
libwebpenc_animencoder : zero initialize the WebPAnimEncoderOptions struct
aiff : check block_align in aiff_read_packet
w32threads : with only 1 thread there cant be a active threading mode .
libswscale / tests / swscale : Fix uninitialized variables
mov : immediately return from mov_fix_index without old index entries
lavfi : avfilter_merge_formats : handle case where inputs are same
lavc / mjpegenc : check av_frame_alloc () failure .
ensure av_rescale_q () can be calculated ( won ' t divide by zero )
videotoolbox : fix crash when decoding interlaced video with new API
avcodec / ituh263dec : Use 0xffff as error code in h263p_decode_umotion ()
mpc8 : Check the seek table size parsed from the bitstream
smoothstreamingenc : explict cast to avoid overflow
alsdec : only adapt order for positive max_order
avcodec / rawdec : check for side data before checking its size
af_channelmap : fix uninitialized variable use introduced in ba8efac977f4276f05274947b2b67d144cbc965a
avcodec / asvdec : Check buf_size
hwcontext_d3d11va : fix crash on frames_init failure
avformat / mpegts : fix iteration count in add_pid_to_pmt ()
vaapi_h264 : Add missing return value check
avformat / nistspheredec : initialize header_size to - 1
vf_overlay : silence warning : X may be used uninitialized in this function
ffplay : fix mem leak when opening input or parsing options fail .
avcodec / interplayvideo : Check that enough data is available for opcode_0x9
avcodec / mpeg4videodec : Fix GMC with videos of dimension 1
bmv : check for len being valid in bmv_decode_frame ().
pca : use sizeof ( variable ) instead of sizeos ( TYPE )
lavd / v4l2 : do not fail when VIDIOC_ENUMSTD returns EINVAL without a valid match
fix double free , priv_data is freed in av_open_input_stream
avprobe : Fix memory leak
h264 : dont stop parsing NALs without cleanup on DPC .
rsd : limit number of channels
ffmpeg : fix sws_dict leak on error exit
avcodec / h264_slice : Clear ref_counts on redundant slices
lavf / tee : fix side data double free .
dca_parser : don ' t overwrite the sample rate , it may not be correct
bitrate sanity check ( fixes assertion failure )
revert r8469 ( increase bits for non default highprecision mode )
lavc : do not leak the internal frame if opening the codec fails
avcodec / utils : Fix undefined behavior in avpriv_toupper4
libtwolame : prevent a NULL pointer dereference
pmpdec : fix integer overflow
lavf : fix null pointer dereference in rdt
mpegts : fix stack array overread in read_sl_header ()
Fixed segfault on memory allocation failure in ape demuxer .
h264 - mt : fix deadlock in packets with multiple slices ( e . g . MP4 ).
lavf / concatdec : fix crash in case a file in script cannot be opened
avcodec / vp56 : Check avctx -> error_concealment before enabling EC
fate / api - codec - param : fix codec context leak
avcodec / jpeglsdec : check shift for values that cause overflow later
avcodec / mjpegdec : make type of shift unsigned to avoid undefined behavior
vf_fps : Fix memory leak introduced by eea64ef4
golomb : Fix overread in get_ur_golomb_jpegls ()
avformat / sdp : fix memleak on error
vf_gradfun : make config_props work properly when called multiple times .
avcodec / xpmdec : there are XPM files with dos line endings
j2kdec : Fix integer overflow leading to a segfault
ffprobe : fix value_string () prefix printing for values with negative logarithm
kill uninitialised variable warning in amr_read_packet ()
avcodec / pcm - dvd : reset last header on errors
avformat / vobsub : fix NULL dereference
cdxl demux : do not create packets with uninitialized data at EOF .
avutil / libm : fix isnan compatibility hack
rtpenc_mpegts : Set chain -> rtp_ctx only after avformat_write_header succeeded
truespeech : check to make sure channels == 1
avcodec / wnv1 : Fix runtime error : left shift of negative value - 1
bfi : fix division by 0
tiff : support reading gray + alpha at 8 bits
lavf / utils : Avoid an overflow for huge negative durations .
avformat / oggparseopus : Free opus extradata before reallocating .
ffplay : check return code of avcodec_decode_video2 ()
fixed one mpeg decoder memory leak
avcodec / mpegvideo_enc : Add missing emms_c () to clear MMX state after SIMD use
avoid integer overflow in dnxhd encoder , fixes # 1557
mmsh : fix integer overflow in mmsh_seek ()
vc1dec : dont apply the loop filter on fields
svq3 : check the watermark height .
mxf : Fix a possible leak of extradata
avcodec / hevc_ps : Only discard overread VPS if a previous is available
avconv : Replace goto redo on decode fail with continue .
avcodec / vc1 : reset fcm / field_mode in non advanced header parsing
ffplay : disallow seeking before the start of the file
avcodec / h264 : clear cur_pic structure instead of duplicating it in ff_h264_update_thread_context ()
avformat / mov : Fix DoS in read_tfra ()
hls : Free packets when skipping packets when seeking
vc1 : check image height , fix division by 0
avcodec / dirac_dwt : initialize support value for fidelity wavelet
vp3 : avoid buffer overread in coeff decode
mpc8 : check seektable size before attempting to use it .
lavfi / aevalsrc : fix sample rate negotiation , after the last merge
vp3 : Check fps validity more completely
avformat / oggparseogm : Check ff_alloc_extradata () for failure
ffserver_config : Check for failure to allocate FFServerIPAddressACL
wavpackenc : assert on too small buffer
checkasm : vp8 . mc : initialize the full src buffer after ec32574209f
avcodec / mpegvideo : fix null pointer dereference
vp3 - mt : fix deadlock when first frame is not a keyframe .
Remove code that duplicates existing code a few lines down .
avformat / oggparsespeex : Check for extradata allocation failure
init picture structure to avoid uninitialized fields
avcodec / flacdec : avoid undefined shift
avcodec / pixlet : Fixes : undefined shift in av_mod_uintp2 ()
ffmpeg : use isatty () before messing with the terminal state
avformat / asfdec_f : Assert that packet positions match in asf_read_pts ()
ffmpeg_vaapi : Unreference global device before setting it
svq1dec : call avcodec_set_dimensions () after dimensions changed .
avformat / mpc8 : clear buffer padding area
h264 : prevent overreads in intra PCM decoding .
avcodec / tiff : reset geotag_count in free_geotags ()
lavf / cafenc : Only allow mono and stereo for Opus .
avformat / smacker : Check if bufs has been allocated before use
png : use av_mallocz_array () for the zlib zalloc function
lavd / lavfi : fix compiler warning for uninitialized variables
RTMP : fix FD leak in rtmp_open ()
lavf / mov : don ' t read outside frag_index bounds
Avoid a deadlock when decoding wma .
random_seed : allow to block on / dev / random
ffmpeg_opt : Use av_realloc_array ()
proresdec : Properly make sure an index doesn ' t run past the limit
avformat / mov : Fix potential integer overflow in mov_read_keys
cast size to int64_t to avoid overflow
avcodec / webp : Fix signedness in prefix_code check
av_dict_set : fix potential memory leak with AV_DICT_DONT_OVERWRITE
flashsv : check deflateInit () return value
lavc / movtextdec : keep the min size instead of max to fix overread .
mkv : mark corrupted packets and return them
avienc : fix overflow of audio sample count
avcodec / dcaenc : clear bitstream end
icodec : fix leaking pkt on error
avcodec / mlpdsp : Fix undefined shift ff_mlp_pack_output ()
avcodec / kgv1dec : Check that there is enough input for maximum RLE compression
lavc / ffjni : fix uninitialized variable
sapenc : Free AVStream -> info on cleanup
avcodec / rl2 : clear freed pointers
tools / target_dec_fuzzer : Fix return code on open failure
avcodec / scpr : Check for min > max in decompress_p ()
h264 : copy h264qpel dsp context to slice thread copies
avplay : Don ' t try to scale timestamps if the tb isn ' t set
flv : Warn only once
vorbisdec : check codebook entry count
decode : avoid leaks on failure in ff_get_buffer ()
avcodec / pngdec : mark previous_picture as done on end of decode_frame_common ()
libavcodec / libtwolame : fix null pointer dereference
bitpacked : fix potential overflow
pthread : Change a signal to a broadcast as multiple threads
avformat / utils : avoid overflow in compute_chapters_end () with huge durations
avformat / mpegtsenc : Changed Video PES packet length to 0 .
smoothstreamingenc : fix integer overflow
af_channelmap : free old extended_data on reallocation
avcodec / flac_parser : Check for av_malloc () failure
mxfenc : don ' t try to write footer without header
lavfi / abuffer : init the data planes with NULL pointers .
ff_h264_direct_ref_list_init : fix B slice check .
avcodec / dcaadpcm : check for av_malloc () failure
lavc : use av_mallocz to allocate AVFrames .
rtpdec_asf : Don ' t free the payload context in the . close function
vc1 : reset interlaced variables , prevent another bunch of crashes .
movenc : Free the buffer returned by url_close_dyn_buffer , regardless of the size
vf_unsharp : Fix false ‘ src2 ’ may be used uninitialized in this function warning .
rmenc : more than 2 streams are not currently possible .
avcodec / takdec : Fix runtime error : left shift of negative value - 63
mjpegdec : tighten unescaped_buf_size size check , prevent null ptr deref
v4l2 : fix uninitialized variable
lavfi / buffersink : check av_frame_ref () failure .
avformat / mxfdec : Check size to avoid integer overflow in mxf_read_utf16_string ()
avformat / mxfdec : Fix Sign error in mxf_read_primer_pack ()
avcodec / xsubdec : Check that RLE coded image and colors fit in the buffer
avcodec / ass_split : Fix null pointer dereference in ff_ass_style_get ()
matroskadec : reset size when freeing data .
h264 : free the tables and uninitialize the context on flush
avformat / mvdec : Use 64bit for ret to avoid overflow
j2kdec : fix division by zero , check tile dimensions for validity
jv demuxer : prevent video packet size overflow
bintext : fix possible x variable overflow .
omadec : Properly check lengths before incrementing the position
mpeg12 : check for available bits to avoid an infinite loop
avcodec / amrwbdec : Fix division by 0 in voice_factor ()
h264 : check buffer size before accessing it
vaapi_h264 : Set max_num_ref_frames to 1 when not using B frames
avformat / oggparsedaala : Do not leave an invalid value in gpshift
avcodec / wnv1 : clear padding area of rbuf
wma : fix off - by - one in array bounds check .
libvo - amrwbenc : Add braces to shut up gcc warning .
diracdec : check if reference could not be allocated
avidec : fix " avidec . c : 362 : warning : st may be used uninitialized in this function "
lavc / avfft : init context to 0 .
avcodec / lagarith : fix integer overflow
lavc / videotoolboxenc : Fix crash when closing codec after error
avformat / movenchint : use realloc_array () to protect against integer overflow
j2kdec : debug markers support .
avcodec / movtextdec : Fix potential integer overflow
avformat / aacdec : Fix leak in adts_aac_read_packet ()
h264 : Fix a possible overread in decode_nal_units ()
av_tempfile : fix leak in error case
lavc / qdrw : Do not fail decoding valid Quickdraw images .
avcodec / dfa : Fix off by 1 error
avcodec / ituh263dec : Fix runtime error : left shift of negative value - 22
typo me thinks ...
avcodec / hevc_cabac : Fix integer overflow in ff_hevc_cu_qp_delta_abs ()
wtv : Mark attachment with a negative stream id
Fix out of bound reads in the QDM2 decoder .
ffm_seek : fix division by zero
h264 : Fix assignments in if ()
a64multienc : change mc_frame_counter to unsigned
indeo4 : prevent printing uninitialized variable
xxan : fix invalid memory access in xan_decode_frame_type0 ()
swscale : enforce a minimum filtersize .
ffplay : zero initialize copy avpacket
avcodec / libxvid : Fix use of uninitialized AVPacket fields
update_frame_pool : use channel field
tm2dec : fix overread
diracdec : avoid overflow of bytes * 8 in decode_lowdelay
flacdec : Silence false positive warning about uninitialized variables in
avcodec / xan : Check for bitstream end in xan_huffman_decode ()
avformat / sierravmd : check for memory allocation failures
yadif : check input video size .
avformat / msf : Also check the codec tag in probing
mcdec : suppress " warning : a / vst may be used uninitialized in this function "
avformat / utils : free AVStream . codec properly in free_stream ()
wmv1 : check that the input buffer is large enough
avio : check AVIOContext malloc failure
avformat / ftp : Solve a crash bug when network occur a exception
lavf : Avoid using av_malloc ( 0 ) in av_dump_format
avcodec / wavpack : Fix 280 : 22 : runtime error : left shift of negative value - 1
binkaudio : fix channel count check
avformat / img2dec : zero probe buffer
lavf / concatdec : do not transfer custom IO flag
avcodec / opusdec : check alignment , misalignment could lead to crashes with avx
dvbsubdec : fix division by zero in compute_default_clut
ffprobe : replace av_destruct_packet () with av_free_packet ()
lavf / mov : Do not overread iTunes metadata .
prevent integer overflow in calculating duration
avcodec / ivi_common : use av_mallocz () to allocate mbs array
libopenjpegdec : always check image because decoding may still fail
pictordec : pass correct context to avpriv_request_sample
msmpeg4dec : check w / h , prevent assert failure later
avcodec / g2meet : check framebuf has been allocated before use
flvdec : fix creation of lots of phantom data streams
h261 : check mtype .
lavc / jpeglsenc : fix allocation in case of negative linesize , and add malloc check
mp3dec : Fix possibly exploitable crash
matroskadec : add assert on lack of overflow in pkt_size + offset
graphparser : add a NULL check on the argument passed to strstr
idcin : check for integer overflow when calling av_get_packet ()
rtspdec : Fix use of uninitialized byte
check entries against field_size , potential malloc overflow in read_stsz , fix # 1357
westwood_vqa : fix null pointer dereference
libavcodec / util : Fix timebase overflow check
zmbv : Reset the decoder on keyframe errors
libx264 : zero x264_picture before use .
avcodec / ra144dec : Fix runtime error : left shift of negative value - 17
wmapro : error out on impossible scale factor offsets
hevc : use av_mallocz () for allocating tab_ipm
avresample : Make sure the even check does not overflow
avutil / mem : Fix flipped condition
ffplay : do not drain existing filters when seeking
fixup_vorbis_headers : add missing malloc failure check
avcodec / dcaadpcm : fix use of uninitialized variable
bitstream : fix a crash in case of oom
avcodec / dxv : Check remaining bytes in dxv_decompress_raw ()
truemotion2 : error out if the huffman tree has no nodes .
avcodec / gdv : Fix undefined shift
vf_gradfun : fix uninitialized variable use
postproc / postprocess : Check for av_mallocz () failure
mpegvideo : Avoid 32 - bit wrapping of linesize multiplications
idcin : check chunk_size value before using it
lavfi / mp : fix invalid read with filters with no argument .
avcodec / jpeg2000dwt : Fix runtime error : left shift of negative value - 123
fix memleak in packet corrupt dropping code
matroskadec : set cur_dts after seek
avformat / omadec : only compute timestamps based on bitrate if its set
avcodec / hevc : Dont turn 32bit timebases into negative numbers
tools / target_dec_fuzzer : Remove FuzzerInterface . h dependancy
xavs : Do not try to set the bitrate tolerance without a bitrate
binkaudio : fix channel count check
avformat / apngdec : Use 64bit for ret to avoid overflow
lagarith : avoid infinite loop in lag_rac_refill ()
vp3 - mt : fix deadlock when first frame is not a keyframe .
dpcm : Fix invalid writes
lavf / http : Remove superfluous parenthesis .
lavd / xv : fix memory leak
id3v2 : fix compiler warning for uninitialized variables
ac3dec : ensure get_buffer () gets a buffer for the correct number of channels
avformat / movenc : Check packet in mov_write_single_packet () too
avformat / http : fix memory leak in parse_cookie .
jpeg2000 : Check that nreslevels2decode has been initialized before use
aacdec : fix signed overflows in lcg_random ()
lavfi / volume : fix NULL reference in filter_frame ()
avcodec / tiff : Fix use of uninitialized off variable
rle_code can overflow when multiplied by 4
v4l2_m2m : Fix integer overflow in timestamp handling
avutil / timecode : Fix fps check
mpegvideo_enc : remove assert that has become obsolete with the new API
avformat / svag : fix division by zero
avcodec / xan : Improve overlapping check
avutil / mathematics : make av_gcd more robust
avcodec / mjpegdec : Fix runtime error : left shift of negative value - 511
indeo4 : fix out - of - bounds function call .
avcodec / diracdec : Fix Assertion frame -> buf [ 0 ] failed at libavcodec / decode . c : 610
lavc : fix recoded subtitles end .
ffv1 : make sure gob_count is not 0
pmpdec : make i unsigned , avoid undefined behavior of i ++
aacdec : Fix an off - by - one overwrite when switching to LTP profile from MAIN .
avformat / oggparseogm : check input size before reading t
avutil / mathematics : Fix division by 0
avformat / nutdec : Check ff_gen_search () for failure
pthread_frame : don ' t sync items between threads for intra - only codecs .
mpegaudiodec : fix short_start calculation
avcodec / wavpack : Change wp_log2 () to unsigned
pngdec : do not release buffer on failure instead report full progress
swresample : fix negative rematrix volumns
tiny_psnr : fix range calculation for sample size of 32 bits
libfaac : Fix assignments in if ()
utils : Do not expand a macro with ' defined ' in it
avcodec / wma : Print more details in case of spectral RLE overflows
tta : fix off be 1 error in the end detection .
lavf / id3v2 : do not export empty fields .
libavformat / flvdec . c : free always the packet after a resync .
dvbsubdec : handle mode change page state
avutil / internal : Do not enable CHECKED with DEBUG
get_bits : make cache unsigned to eliminate undefined signed overflow .
avcodec / snowenc : Clear MMX state after edge drawing and picture encode
flvdec : Fix compiler warning for uninitialized variables
avcodec / ass : check for av_mallocz () failure
ffmpeg : Replace goto redo on decode fail with continue .
lavf / segment : exit immediately in case of invalid stream specifier
fix mem leak
avformat / rtmppkt : Fix random crash
avoid infinite loop if pixel format conversion does not exist
avcodec / mpegaudiodec_template : Fix 2 runtime error : signed integer overflow
avconv : don ' t pass a bogus parameter to avfilter_graph_create_filter ().
ffmpeg : fix uninitialized variable warning
indeo3 : fix data size check
tta : check for extradata allocation failure in tta demuxer
lavc / avpacket : Fix undefined behaviour , do not pass a null pointer to memcpy ().
avcodec / cbs_jpeg : Check length for SOS
avformat / mpegenc : improve field names in underflow error message
vf_delogo : fix an uninitialized read .
avcodec / jpeglsdec : Check for end of bitstream in ls_decode_line ()
libopencore - amrdec : allow sampling rates other than 8khz
tiffdec : Prevent illegal memory access caused by recycled pointers .
avformat / mpc8 : fix hang with fuzzed file
avcodec / svq3 : use av_fast_padded_malloc ()
lavr : do not try to copy to uninitialized output audio data .
avformat / mov : Check creation_time for overflow
utils / update_stream_timings : fix division by zero
avplay : Do not print a possibly uninitialized value
adpcm : Check for channels to be a non - zero integer
mov : fix leaking memory with multiple drefs .
avformat / img2dec : do not rewind custom io buffers
rtmpproto : make condition work in overflow case .
avcodec / snowdec : Check for remaining bitstream in decode_blocks ()
apetag : fix error handling in ff_ape_parse_tag ()
avformat / oggparsetheora : zero extradata padding area
avcodec / lagarith : reenable buggy lag_decode_zero_run_line ()
avcodec / shorten : Check residual size
mpegpsenc : Fix integer overflow of the muxrate calculation .
fate / cabac : replace uninitialized bytes by random bytes
mpegaudidec : 10l forgot buf_size
avcodec / mpegaudiodec_template : Fix multiple runtime error : signed integer overflow
avcodec / libtheoraenc : Check for av_malloc failure
avformat / mpegenc : Better heuristic for ordering packets
avcodec / mjpegdec : Clip DC also on the negative side .
avformat / mov : Fix signed integer overflows with total_size
shorten : pad the internal bitstream buffer
sws : fix assert failure
cook : Make constants passed to AV_BE2NE32C () unsigned to avoid signed overflow .
ffprobe : fix leak in show_packets ()
flvenc : silence bogus warning
ffprobe : set writer context to 0 after allocation
h264 : check that execute_decode_slices () is not called too many times
sonicdec : fix frame size
adxdec : Fix division by zero
avcodec / ac3enc : fix undefined negative left shift
snow : zero scratchbuf .
avprobe : Unref the packet once it is used
vfwcap : support MJPG compressed streams .
avcodec / tiff : Clear deinvert_buf_size on deallocation
h264 : check for integer overflow , fix null pointer dereference
avutil / tree : fix memleaks
hlsenc : make segment number unsigned
hevc_ps : make sure failing to decode an SPS always returns an error
huffyuv : error out on bit overrun .
avcodec / mpegvideo : make mc_mb_var_sum_temp / mb_var_sum_temp 64bit
avformat / dss : Use av_freep () to avoid leaving stale pointers in memory
jvdec : check videosize
avformat : Fix Pro - MPEG non - square matrix
asfdec_o : prevent overflow causing seekback
avformat / test / fifo_muxer : add check for FailingMuxerPacketData alloc
avcodec / samidec : check av_strdup () return value
lavf / dnxhd : Autodetect more valid files .
avcodec / lossless_videodsp : add_hfyu_left_prediction_int16_c : fix harmless integer overflow
avformat / avidec : Check nb_streams in read_gab2_sub ()
avformat / dsfdec : set bit_rate , fixes duration estimation
avformat / jvdec : clear packet padding after downsize
avcodec / tdsc : use ff_codec_open2_recursive ()
proresdec_lgpl : explicitly check coff [ 3 ] against slice_data_size
vp9 : reset segmentation information on context reset .
mxfdec : Fix a potential DoS vector in mxf_read_pixel_layout ()
vorbisdec : fix heap buffer overflow .
on2avc : Fix out of array access
avformat / mp3dec : fix msan warning when verifying mpa header
avcodec / aac : fix () in IS_CODEBOOK_UNSIGNED macro
avcodec / h264_slice : don ' t sync default_ref [] between threads .
avformat / vqf : check avio_read () s return value more completely
applehttp : Avoid reading uninitialized memory
avcodec / pixlet : fixes integer overflow in read_highpass ()
indeo4 : Skip null frames .
crash fix by Carl Eugen Hoyos cehoyos ag . or at
avformat / hevc : fix mix of av_malloc () with free ()
movdec : Free the previous extradata
avformat / ape : Use av_malloc_array ()
h264 : fix variable overflow after a few years of video playback
rtp : Fix integer underflow that could allow remote code execution .
lavu : test for broken binutils on ARM
dcadec : Always initialize return variable
hls : do not access pb -> opaque for custom IO .
h2645_parse : don ' t overread AnnexB NALs within an avc stream
roqaudioenc : Fix crash with very small roq files
libavcodec / qsvenc . c : A warning message when library will work at partial hardware acceleration .
check region depth for validity
Do a little more checking .
h264 : on reference overflow , reset the reference count to 0 , not 1 .
matroskadec : fix a sanity check .
latmdec : Check AudioSpecificConfig length before decoding extradata .
af_amix : prevent memory leak on error path
avutil / opt : fix av_log type
vp56 : check buffer size to fix a potential segfault
lavu / error : fix sign error in av_sterror () error code
lavf / mov : fix crash in mov_read_sidx
avcodec / bsf : fix resource leak in av_bsf_list_parse_str
wmadec : Verify bitstream size makes sense before calling init_get_bits .
avcodec / wavpackenc : print channel count in av_log call
avformat / iff : shrink packets to the initialized data
avcodec / pixlet : check out of bounds pfx value
indeo5 : Fix null pointer dereference
ffplay : only update pts if not redisplaying a frame
x86 : ac3dsp : Remove 3dnow version of ff_ac3_extract_exponents
vf_mp : fix null ptr deref in case of ENOMEM
avformat / matroskadec : fix off by 1 error in matroska_read_seek ()
avcodec_encode_audio2 : Increase the audio buffer size .
lavf / segment : remove duplicated and inconsistent cleanup code in seg_write_packet ()
avcodec / cinepak : fix integer underflow
hevc : unref the current frame if frame_start () fails
makes really sure that no undefined behavior can happen
avcodec / xwddec : Check bpp more completely
wmalosslessdec : fix mclms_coeffs * array size
avformat / movenc : Do not pass AVCodecParameters in avpriv_request_sample
avresample : prevent theoretical division by zero
avcodec / eamad : silence uninitialized variable warnings
avcodec / wavpack : Fix undefined integer negation
aiffdec : fix division by zero
avformat / matroskadec : use av_realloc_array ()
buffersrc : fix invalid read in uninit if the fifo hasn ' t been allocated
Prevent scalefactors from overflowing .
avcodec / webp : Fix null pointer dereference
mov : detect EOF in mov_read_dref ()
aacsbr : fix integer overflow
fix sigfpe with intra only encoding
mxfdec : return error if no segments are available in mxf_get_sorted_table_segments
avs : silence " may be used uninitialized " warnings
matroskadec : prevent access of elements after freeing
pthreads : reset got_frames on flush .
avformat / hlsenc : Fix a memory leak when http_persistent is 1
fixing mem corruption
fix mem leak
avformat / dump : Fix integer overflow in av_dump_format ()
avcodec / shorten : Check k in get_uint ()
avformat / mxfdec : Fix memleak of packages_refs
golomb : always check for invalid UE golomb codes in get_ue_golomb
avcodec / smvjpegdec : return the packet size instead of 0
avcodec / dvdsubdec : error on bitmaps with size 0
avformat / idcin : Use 64bit for ret to avoid overflow
rtsp : Fix compiler warning for uninitialized variable
hwcontext_dxva2 : initialize D3DDISPLAYMODEEX correctly
avcodec / pngenc : Check that there is at least 1 frame
aacenc : fix out of array writes
avcodec / hevc_ps : prevent stale pointer in malloc failure case
remove warnings about uninitialized vars
vorbis : Fix last quarter of CVE - 2011 - 3893
avcodec / cllc : Check prefix
g723_1dec : remove unneeded cliping that leaked in from merge from libav
lavf / audiointerleave : check for allocation failure
libavformat / mov : Fix memory leak , need to free the chapter tracks array
wtvdec : fix integer overflow resulting in errors with large files
avformat / 4xm : shrink packet if it was only partially initialized
avformat / utils : check for malloc failure
lavf : avformat_seek_file (): validate stream_index .
qdm2 : fix signed multiplication overflow
avpriv_mpeg4audio_get_config : check init_get_bits () return code .
avcodec / vp8 : Fix null pointer dereference in ff_vp8_decode_free ()
avcodec / fmvc : Fix off by 1 error
ffmpeg : Clear error message array at init .
lavc / ffjni : fix local reference leak
imgconvert - test : add avg bits per pixel
vorbisdec : do not leak the first frame .
avutil / frame : Copy size = 0 side data in ff_init_buffer_info ()
mpegvideo_enc : remove assert that has become obsolete with the new API
avcodec_encode_audio (): fix invalid free
ffmpeg : disable autoinsertion for - async on the output side of the filtergraph .
avcodec / jpeg2000dec : zero Jpeg2000QuantStyle structure before use in get_qcd ()
uninitialized var ...
dump_stream_format : fix division by 0
lavfi / fade : Do not overread input buffer .
hdsenc : Fix an off by one error in an array size check
lavfi / showwaves : do not set on outlink -> out_buf the sent video frame
lavf : Make sure avg_frame_rate can be calculated without integer overflow
fix exploitable buffer overflow
avcodec / mpeg12dec : check for overread in mpeg1_fast_decode_block_inter ()
use filter name when graph parser add filters
lavfi / alphamerge : fix leak introduced in 217163eb
avf_showwaves : fix null pointer dereference
shorten : Fix out of bound writes in fix_bitshift ()
postproc / postprocess : Check for av_malloc () failure
avcodec / webvttdec : Fix uninitialized use of variable " again "
sbgdec : dont set slide to a uninitialized value
matroskadec : fix a memory leak
svq1dec : dont export the qscale table .
avutil / hwcontext_dxva2 : return an error when buffer allocation fails
ffmpeg : Check for subtitle_out malloc failure
avcodec / wmalosslessdec : deallocate uninitialized frame on decode_tilehdr () failure
xwddec : prevent overflow of lsize * avctx -> height
avcodec / sheervideo : Check input buffer size before allocating and decoding
atrac3 : Fix crash in tonal component decoding .
id3v2 : allocate large enough buffer
avcodec / snowdec : Check qbias
timecode : fix typo
avformat / rtsp : Fix potential pointer overflow in sdp_probe ()
lagarith : check count before writing zeros .
lavc / libopenjpegenc : check av_frame_alloc () failure .
decode : Initialize ret before using it
mxg : fix compiler warning for uninitialized variables
avcodec / wmalosslessdec : shrink output on error so no uninitialized data is returned
cbs_h265 : Fix reading of unknown parameter set extension data
vc1dec : prevent memory leak in error path
svq3 : Avoid a division by zero
ffmpeg : fix a crash with complex filters when pix_fmt is not specified
oggenc : add missing initializer braces to shut up gcc warning .
h264 : Mark previous field as done before throwing it away .
avcodec / h264_mp4toannexb_bsf : Reorder operations in nal_size check
avformat / nutdec : Check X in 2nd branch of index reading
lavc : add 422 / 444 YUV with alpha to align_dimensions ()
mpc7 : check for allocation failure
msmpeg4 : Don ' t set up run - level info for level 0 .
matroskadec : avoid potential mem leak
avcodec / libstagefright : Check for pthread_create () failure
vaapi_h264 : Scale log2_max_pic_order_cnt_lsb with max_b_frames
ff_msmpeg4_decode_init () calls ff_h263_decode_init () which calls
jpeg2000 : zero reslevel array on allocation
ac3dsp : fix loop condition in ac3_update_bap_counts_c ()
avformat / msf : Check channels when reading the header
rtpdec_h264 : Free old extradata before clearing the pointer
ffserver : Clear avio context after closing it
avcodec / mjpegdec : Check escape sequence validity
avcodec / dca_lbr : Fix " warning : missing braces around initializer "
aacdec / lcg_random : fix harmless integer overflow
avcodec / hevc : make * ps_id unsigned
mpegenc : simplify muxrate calculation
check udp_read_packet return value not to access uninitialized memory
avcodec / mlpdsp : Fix signed integer overflow , 2nd try
avidec : use actually read size instead of requested size
avcodec / hevc_cabac : Check prefix so as to avoid invalid shifts in coeff_abs_level_remaining_decode ()
msmpeg4enc : fix possible use of uninitialized warning
Values > 4 for lowres corrupt output .
yuv2rgb : fix integer overflow in fill_table
av_image_copy : check for av_image_get_linesize () failure .
vf_pad : check that we have write permission before writing in the buffer .
avcodec / hevc_ps : Check return code from pps_range_extensions ()
lavf / swf : Fix auto - detection of compressed files .
put_bits : fix invalid shift by 32 in flush_put_bits ()
avpacket : fix size check in packet_alloc
wmalosslessdec : Fix reading too many bits in decode_channel_residues ()
mpegvideo_enc : initialize the encoding context
libx264 : fix double free
avformat / mux : Apply auto bsfs in av_write_frame () too
avcodec / flacdec : Fix undefined shift in decode_subframe ()
avcodec / exr : fix null pointer dereference
indeo3 : error out if no motion vector is set .
Fixed a possible bug , checked against the assembly .
dsicin : Add some basic sanity checks for fields read from the file
avcodec / cavsdec : Fix undefined behavior from integer overflow
vp9_raw_reorder_bsf : Remove a redundant allocation
avformat / vqf : Use 64bit for ret to avoid overflow
avcodec / jpeg2000dec : Zero code and quant style
avcodec / libfdk - aacdec : Correct buffer_size parameter
pngdec : Fix interlaced PAETH prediction
oggparseskeleton : Check the overall start time before using it .
rawdec : propagate pict_type information to the output frame
segafilm : fix division by 0
shorten : dont leave invalid channel counts in the context .
Check for out of bound reads in the Electronic Arts CMV decoder .
targa : prevent integer overflow in bufsize check .
apetag : propagate errors .
vp8 : avoid race condition on segment map .
dca : Fix uninitialized variable warnings .
avcodec / pgssubdec : Use av_fast_padded_malloc () for rle buffer
matroskadec : don ' t warn about unknown spherical medata when none is present
loco : silence warning : decoded may be used uninitialized in this function
avformat / segafilm : check 2nd tag
rv20 : prevent calling ff_h263_decode_mba () with unset height / width
avcodec / utils : Check close before calling it
sgidec : safer check for buffer overflow
asf : reset side data elements on packet copy .
pcmdec : check that channels is valid .
* fix double free
Fixed invalid access in wavpack decoder on corrupted extra bits sub - blocks .
sol : return error if av_get_packet () fails .
iff : validate CMAP palette size
rdt : Check the return value of avformat_open
lavfi / fifo : add assert to ensure request was successfull .
ape : Use the proper variable type
ffmpeg : avoid NULL dereference with filters .
swscale : fix 16 - bit horizontal scaling underflow .
lavf / img2dec : Initialize a stack variable .
avformat / lrcdec : Fix memory leak in lrc_read_header ()
audemux : Add a sanity check for the number of channels
avcodec / truemotion2rt : also export key frame info
protect realloc overflow
asfenc : fix leaking asf -> index_ptr on error
mpc8 : check stream count before accessing stream 1 .
dvbsubdec : add missing break
lavf / mpeg : Initialize a stack variable used by memcmp ().
avcodec / cinepakenc : Zero AVPictures , silence " may be used uninitialized " warnings
flvdec : Inject in stream extradata , into the stream .
ffv1 : check for malloc failure .
avformat / mpegts : reset last_ver on corrupted packets
avformat / sapenc : Use av_mallocz_array ()
aaccoder : prevent crash of anmr coder
avcodec / wavpack : Treat the first block coding too many channels as an error
srtp : Mark a few variables as uninitialized
g723_1 : fix off - by - one error in normalize_bits ()
audemux : Fix potential integer overflow leading to a division by 0
wavpack : Fix an integer overflow
vaapi_encode : Discard output buffer if picture submission fails
ffprobe : check av_asprintf () for failure
mpegts : fix demuxing PES private stream 2
avformat / movenc : Write version 2 of audio atom if channels is not known
libshine : fix support for shine 3 . 0
svq1dec : Fix overread on very small input
avcodec / ff_h264_decode_sei : fix integer overflow with size .
avformat / bintext : protect against potential overflow of chars_per_frame
qtpalette : make the color_ * variables unsigned again
avcodec / snow : Initialize spatial_decomposition_count to a valid value
asink_nullsink : plug a memory leak .
avformat / icodec : Fix crash probing fuzzed file
lavr : check that current_buffer is not NULL before using it
cpu detection : avoid a signed overflow
sanm : remove " duplicate " for loop .
lavfi / separatefields : fix frame leak
libavformat / rtpenc_mpegts : check avformat_new_stream () return value
escape124 : reject codebook size 0
ac3dec : apply dynamic range compression to correct channels in dual - mono mode .
libx265 : Fix use of uninitialized input picture
matroskadec : fix potential mem leak
ac3_parser : add required padding for GetBitContext buffer
aac_parser : add required padding for GetBitContext buffer
avcodec / ffv1dec : Fix integer overflow in read_quant_table ()
vc2enc : increase the starting value of the size scaler
avcodec / g2meet : check available space before copying palette
avformat / mov : fix integer overflow in mov_read_udta_string ()
vaapi : return early from ff_vaapi_render_picture () without picture
atrac3 : Fix crash in tonal component decoding .
indeo : Reject impossible FRAMETYPE_NULL
lagarith : pad RGB buffer by 1 byte .
bethsoftvid : check return value of av_packet_new_side_data ()
apetag : do not leak memory if avio_read () fails
matroskadec : fix leak on error
avcodec / utils : Fix potential overflow in overallocation code
hnm4 : change width / height to int to fix hypothetical integer overflows
rtmp : Clean up properly if the handshake failed
avformat / oggparseopus : Fix Undefined behavior in oggparseopus . c and libavformat / utils . c
quickdraw : Skip the empty 512 byte header for images
utils . c : fix crash with threading enabled .
lavf / utils : fix mem leak
fix initial vbv buffer underflow
avformat / libquvi : Fix whitelist handling
af_join : do not leak input frames .
avcodec / flacenc : Fix Invalid Rice order
mpegpsmux : Fix integer overflow with huge VBV sizes .
jvdec : check that the video_size fits in the packet .
doc / examples / filtering_audio : init packet0 . data
avcodec / cllc : Check bitstream end before decoding pixels
avformat / rmdec : check against mismatching int4 interleaver parameters which would leave uninitialized holes
avformat / rtpenc : check av_packet_get_side_data () return , fix null ptr dereference
rtsp : Fix compiler warning for uninitialized variable .
mpegenc : prevent a NULL pointer dereference
motionpixels : Prevent calling init_vlc () with invalid parameters
avcodec / ac3dec_fixed : Fix integer overflow in scale_coefs ()
avcodec / dirac_arith : fix integer overflow
crypto : Use av_freep instead of av_free
h264 : don ' t initialize missing pictures when using a hwaccel
idroqdec : fix leaking pkt on failure
mjpegenc : Remove duplicate initializer
udp : do not call pthread_ { mutex , cond } _destroy when not initialized .
avcodec / h264 : Clear delayed_pic on deallocation
vp9 : make mv bounds 32bit .
avcodec / wmalosslessdec : fix mclms_coeffs * array size
lavc / options : fix shallow copy context
rtmpproto : Validate the embedded flv packet size before copying
rtsp : Don ' t use uninitialized data if there are no streams
avcodec / diracdec : Check perspective_exp and zrs_exp .
ffprobe : fix use of uninitialized pointer in av_strtok ()
avcodec / vp3 : check current_frame before accessing it
mpegts : Fix potential memory leak in mpegts_read_packet ()
fixed long standing off - by - one bug ( fixes playback on ppc )
libx265 : Fix use of uninitialized input picture
avcodec / jpeg2000dec : Check SIZ dimensions to be within the supported range
wmaprodec : check num_sfb for validity . Fix out of array accesses
sdp : Check that fmt -> oformat is non - null before accessing it
matroskadec : fix out of bounds write
shorten : allocate space for padding
lavf / utils : Do not force chapter end time before chapter start .
avcodec / aacdec_fixed : Fix integer overflow in apply_dependent_coupling_fixed ()
lavf / utils : ignore subtitle timestamps in timings calculation
xan : Prevent NULL dereferences with missing reference frame
pthread_frame : Unreference hw_frames_ctx on per - thread codec contexts
drawtext : draw glyphs for the strftime ()- expanded text
libavformat / hdsenc : check init_file () return code
examples / filtering_audio : fix a memory leak .
af_asyncts : avoid overflow in out_size with large delta values
avformat / ipmovie : Check that the OPCODE_INIT_AUDIO_BUFFERS size is large enough
ac3enc : simplify sym_quant ()
ffmpeg : Check read_ffserver_streams () return value
Free the RTSPStreams in ff_rtsp_close_streams
avcodec / interplayvideo : Fix timeout from lack of bitstream end check
applehttp : fix variant discard logic
dds : disable palette flag for compressed images
lavc / audiotoolboxdec : fix memory leak
libavutil / hwcontext_dxva2 : Add check for possible errors from GetAdapterDisplayModeEx
rmdec : fix crash at end of file
H264 : hotfix for speedloss on frame threading and h264 files with slices .
avformat / mov : zero initialize codec_name in mov_parse_stsd_video ()
Fix a crash on oom when decoding hevc .
avcodec / x86 / dsputil_init : only use xvid idct for lowres = 0
fix mem leak
avformat / mov : clear padding area in mov_read_extradata ()
avformat / flvdec : initialize context before reading from it
rv30 : check block type validity
avcodec / mmvideo : Fix undefined behavior ( left shift of negative value )
avcodec / libxavs : Use av_mallocz_array ()
avcodec / mpeg12dec : Fix runtime error : left shift of negative value - 1
avformat / mpeg : zero initialize idx_pkt
mpeg12 : terminate mpeg2_video_profiles arrays
Remove lowres support from wmv2 decoder
opt : check memory allocation
indeo4 : check ref_mb
avcodec / diracdec : Fix integer overflow in divide3 ()
avcodec / tiff : remove byte based bpp special case
opt : fix memleak on error of range_array
avutil / opt : ensure the right buffer is used in set_string_number ()
flvdec : skip duplicate indexes
avcodec / mlpdec : Check that there is enough data for headers
avcodec / utils : Align YUV411 by as much as the other YUV variants
libxvid_rc : fix leaks in ff_xvid_rate_control_init ()
avutil / hwcontext_d3d11va : Fix leak when wrapping texture in AVD3D11FrameDescriptor
graphparser : add a NULL check on the argument passed to strstr
hdsenc : Avoid integer overflow
mxfdec : Fix files with essence containers larger than 2 GiB .
lpc : use default number of passes when lpc_passes is invalid
rtspdec : read_line : fix use of uninitialized byte
dpx : Make start offset unsigned
allcodecs : move libaacplus down .
tiff : check bppcount
avcodec / nvenc : fix potantially uninitialized free
avcodec / vp9_superframe_bsf : cache packets by creating new references rather than moving them
g726 : Do not try to decode more than one channel .
avformat / takdec : zero padding area before use
mov : reset dref_count on realloc to keep values consistent .
electronicarts : Add more sanity checking for the number of channels
lavfi / deshake : fix crash in uninit ()
txddemux : Limit allocated packets to filesize .
http : Make sure proxyauth is initialized
vorbisdec : avoid invalid memory access
imc : Catch a division by zero
lavc / opusdec : Fix a memleak when reading invalid files .
mpegaudiodec : Enable checked bitstream reader .
avcodec / jpeglsdec : Check ilv for being a supported value
avformat / iff : check avio_read () return in get_metadata ()
h264 : fix clang warning about uninitialized variable
avidec : Free packet if dv_produce_packet fails .
dsicinav : Check for out of bounds writes
xwddec : prevent overflow of lsize * avctx -> height
perform sanity check on number of channels in electronicarts demuxer
avcodec / avpacket : zero memory in av_packet_new_side_data ()
lavfi / gradfun : avoid use of uninitialized variable .
Avoid a null pointer dereference on clean - up after oom in ac3 encoder .
cbs_h2645 : Fix memory leak on when reading SEI fails
avcodec / hevc_ps : Fix c ? _qp_offset_list size
avcodec / gdv : Check available space before reading palette
avoid possibly uninitialized return value
audio_mix : initialize the data pointers to NULL
avcodec / avpacket : Fix memory allocation failure check
mss3 : prevent AC state from becoming invalid in rac_normalise ()
avformat / smoothstreamingenc : Use av_mallocz_array ()
diracdec : clear slice_params_num_buf on allocation failure
smush : check audio packet size
avcodec / mpegvideo_enc : Fix leak on bitstream buffer reallocation
VP3 : Prevent stack corruption from an unset custom coding method .
mem : Consistently return NULL for av_malloc ( 0 )
avidec : add missing initializer braces to shut up gcc warning .
seek back at the end of file after updating header
Fix a crash when reading gray pam files .
avcodec / magicyuv : set correct size of last slice for each plane
avformat / ivfenc : fix division by zero
avcodec / dnxhddec : Do not overwrite colorspace if the container has set it .
lavc / videotoolboxenc : fix open videotoolbox bug on iOS8 . 4
movenc : Check that frag_info entries exist in mov_write_sidx_tag
tcp : Check the return value from getsockopt
lzw ( gif ): Fix overread
get_bits : change the failure condition in init_get_bits
targa : prevent integer overflow in bufsize check .
avcodec / flac_parser : Update nb_headers_buffered
avutil / eval : check for av_malloc failure
oops , potential overflow on really large blocks
avcodec / alsdec : Check for overread
kill uninitialised variable warning in ac3_probe ()
avcodec / cbs_jpeg : Check length for SOS
lavc / mjpegdec : Do not overread too short JFIF tags .
h264 : fix () placement
aacenc : Prevent premature termination of the two loop search .
avformat / movenc : Check that frame_types other than EAC3_FRAME_TYPE_INDEPENDENT have a supported substream id
flashsv : make sure data for zlib priming is available
motionpixels : check extradata size
avformat / oggparseogm : Check available data before reading global header
avcodec / cavsdec : Check esc_code
xbmdec : fix off by one error in scanf ()
dfa : Put our pointer check back .
rtsp : Free the rtpdec context properly
rawdec : don ' t leak option strings .
avformat / matroskaenc : add missing allocation failure checks for stream durations
avcodec / h264 / find_start_code : factorize addition out
vaapi_encode : Discard output buffer if picture submission fails
yopdec : check frame oddness to be within supported limits
avcodec / imc : dont read bits beyond the end
qsvdec : Drop stray extra braces around initializer
error concealment : check last pictures motion_val before using it .
lavc / cdxl : clear palette before reading it
avformat / apngenc : use av_packet_alloc ()
avcodec / iff : warn about truncated input to decode_byterun () and clear remaining output
libavcodec / x86 / h264dsp_mmx . c : add forgotten HAVE_YASM
avformat / tls_schannel : Fix use of uninitialized variable
sdp : Fix null pointer dereference with aac and ffserver .
crypto : Use av_freep instead of av_free
lavf : fix invalid reads in avformat_find_stream_info ()
mpegvideo : unref buffers in ff_mpeg_unref_picture on frame size changes
ffmpeg : Fix null pointer dereference in do_video_out ()
avcodec / apedec : use av_fast_padded_malloc ()
mpeg12enc : Don ' t set up run - level info for level 0 .
avcodec / dirac_vlc : Fix invalid shift
avcodec / h264_cavlc : Fix undefined behavior on qscale overflow
avformat / utils : Check AVFormatContext -> duration in compute_chapters_end () before use
ac3enc_template : silence may be used uninitialized in this function warnings
avcodec / libtheoraenc : Check for stats allocation failure
lavf / tee : fix leak of select option .
dvenc : dont fail hard if the timecode is invalid
softfloat : handle - INT_MAX correctly
vf_paletteuse : Don ' t free the second frame from ff_framesync_dualinput_get_writable on error
Avoid a null pointer dereference on oom when decoding vc1 .
sws : Fix warning : ‘ abase ’ may be used uninitialized in this function
cook : fix use of uninitialized variable
h264 : reset current_slice on context reinit
avformat / rmdec : Fix Packet memleak at close ()
threads : fix a potential race spotted by helgrind .
avcodec / utils : Clear dimensions in ff_get_buffer () on failure
vf_drawtext : fix memory leak of glyph data .
avcodec / hevc : fix nb_sh / nb_sps check
av_probe_input_format3 : support NULL as buffer . Fixes null ptr deref
sanm : fix undefined behaviour on big - endian .
amrwbdec : Decode the fr_quality bit properly
avcodec / mjpegdec : Add some sanity checks to ljpeg_decode_rgb_scan ()
avcodec / ituh263dec : Check for the bitstream end in ff_h263_decode_mb ()
libtheora : Check frame allocation
avcodec : disallow reget_buffer () if pix_fmt changed .
avformat / isom : zero MPEG4AudioConfig to avoid leaving fields uninitialized
mmvideo : fix uninitialized variable use in mm_decode_intra
rtsp : Set the real_setup pointer properly , avoid out of bounds memory accesses
mp3on4 : fix null pointer dereference
avcodec / utvideodec : Fix bytes left check in decode_frame ()
avformat / mov : fix memory leak
ividsp : zero pitch so as not to overread
avformat / rmdec : check that sub_packet_size fit in the packet for DEINT_ID_GENR
Fix a leak in the AAC encoder
avcodec / huffyuvdec : do not leave alpha values uninitialized
fraps : check for overread .
ape : stop reading after the last frame has been read
movtextenc : Remove dangling reference to movtext . h
avconv : initialize hwaccel_pix_fmt
avformat / utils : fix a codecpar non use
avcodec / sbrdsp_fixed : Fix integer overflow
mov : fix self referencing timecode tracks
avconv : fix off by one check in complex_filter
qsvdec : Fix memory leak
lavc / ass_split : check for NULL pointer in ff_ass_split_override_codes ().
imc : Catch a division by zero
avcodec / diracdec : Fix off by 1 error in quant check
ffv1 : Make sure at least one slice context is initialized
wma : Clip WMA1 and WMA2 frame length to 11 bits .
diracdec : Use only one frame per reference
lavc / options : fix leaks in avcodec_free_context
avformat / librtmp : Fix memory leak if RTMP_ConnectStream () fails
avcodec / sgidec : fix count check
lavf : prevent crash in av_open_input_file () if ap == NULL .
rtsp : Check for command strings without spaces
avcodec / ffv1dec : Clear quant_table_count if its invalid
segafilm : Check for memory allocation failures in segafilm demuxer .
tiff : set the correct return value when check_size () fails
avcodec / eamad : use av_freep (), do not leave stale pointers in memory
avcodec / mjpegdec : Check for end for both bytes in unescaping
h264 : Make sure reinit failures mark the context as not initialized
avformat / rmdec : Fix use of uninitialized variable
swscale : fix NULL checking in sws_alloc_context ()
avcodec / h264_refs : Change default case to av_assert0 () to suppress warning
libavutil / opencl : fix potential null dereference
avformat / mvdec : Fix DoS due to lack of eof check
libfdk - aacdec : Fix a boundary check
vsrc_movie : fix leak in request_frame ()
vp8 : prevent read from uninitialized memory in decode_mvs
libavformat / cdxl : fix duration in case of overflow
avformat / mp3dec : Check for avcodec_alloc_context3 () failure
matroska : set " done " only during resync fail .
check for malloc failure
avcodec / cfhd : Fix invalid left shift of negative value
dont reduce the min_distance of AVIndexEntry
truemotion2 : clear the token array if its initialization fails .
indeo3 : Fix overreading requant_tab .
avcodec / mpegaudioenc_template : dont crash on bitrate = 0
opus_pvq : remove unneeded assert
avcodec / asvdec : Use rounded up dimenensions in input size check
mjpegbdec : dont return a picture when there is no picture .
ffmpeg_cleanup : fix crash with unrecognized codec
elsdec : Replace EOVERFLOW with INVALIDDATA
vfwcap : Add support for UYVY pixel format .
avcodec / nuv : zero buffer padding
avcodec / shorten : if allocation fails reset max_frame_size
wmavoice : fix a signed overflow
indeo5 : Fix null pointer dereference .
avcodec / vc1dec : fix null pointer dereference
mxfdec : free descriptor extradata in . read_close ()
lavfi / asyncts : Fix compilation with GCC 4 . 7
lavfi / tinterlace : set inlink -> cur to NULL , since it is stored internally
ffmpeg : avoid possible undefined behavior
ffmpeg : Dont crash on unconnected output pads before an error is printed
ttadec : check channel count as read from extradata .
vaapi : return early from ff_vaapi_render_picture () without picture
vsrc_movie : do not free avoption variables in uninit ()
vf_format : check input validity
matroska : honor error_recognition on unknown doctypes
avcodec / jpeg2000dwt : Fix integer overflow in dwt_decode97_int ()
ffv1enc : Fix memleak
hls : fix leaking avio_opts on hls_read_header error
avcodec / mpegvideoenc : fix undefined negative left shift
avformat / adxdec : check avctx -> channels for invalid values
crc : fix signed overflow
lavc / h264 : move ff_init_cabac_states () from decode_slice () to ff_h264_decode_init ().
doc / examples / decoder_targeted : Disable error concealment after 20 frames
mov : fix global unicode convertion array overflow .
imc : add required padding for GetBitContext buffer
idcin : fix memleak
flashsv : check for keyframe before using differential coding
Free the RTSPStreams in ff_rtsp_close_streams
hlsproto : fix integer overflow
mxfdec : fix potential integer overflow in mxf_compute_sample_count ()
vsrc_movie : fix leak in request_frame ()
check for error
a64multienc : fix " warning : buf may be used uninitialized in this function "
lavd / v4l2 : do not fail when VIDIOC_ENUMSTD returns ENODATA
lavfi / ocv : fix crash with no filter name specified .
h264 : only assign H264Picture :: mbaff for first slice .
avformat / oggenc : check for stream private data in ogg_free ()
lavfi / subtitles : 10l close codec before format .
avcodec / ffv1dec : update progress in case of broken pointer chains
avcodec / aacdec_fixed : Fix runtime error : left shift of negative value - 1297616
cmdutils_opencl : Fix read of uninitialized pointer
lavf : revert commit bb99ae3ae924c942a634bec7711ec7ee11c38eb9
check for qscale == 0 ( fixes 1 / 0 on one corrupted stream )
swscale : Do not expand a macro with ' defined ' in it
oggdec : fix invalid free on error
avcodec / takdec : Fix runtime error : left shift of negative value - 42
swr : fix " may be used uninitialized in this function " warnings
mpeg12 : terminate mpeg2_video_profiles arrays
opus_pvq : prevent division by 0
msmpeg4v1 : fix undefined behaviour in msmpeg4_decode_picture_header ()
avcodec / shorten : clear bitstream buffer
lagarith : check count before writing zeros .
aacdec : free frame buffer if no audio was decoded
avformat / sbgdec : Check alloc_array_elem () return value
h264 : don ' t re - call ff_h264_direct_ref_list_init () w / frame - mt .
ffmpeg_opt : Check for localtime () failure
remove useless cast that makes code unreadable
avidec : update size when packet is shrunk
png : Use av_freep () instead of av_free ()
avcodec / apedec : tmpk == 32 is not supported , prevent undefined behavior
mov : Free an earlier allocated array if allocating a new one
qcelpdata : correct start bit for cbgain [ 0 ] in qcelp_rate_octave_bitmap []
avpacket : Initialize the allocated padding area in side data
vf_overlay_opencl : Don ' t leak output frame on error
avcodec / wavpack : Fix invalid shift and integer overflow
id3v2 : catch avio_read errors in check_tag
VP8 : add missing free
libschroedingerdec : don ' t produce empty frames
mov : fix heap buffer overflow
g723 . 1 : fix addition overflow
asf : only set index_read if the index contained entries .
avcodec / vp9 : mark frame as finished on decode_tiles () failure
avcodec / h264 : Be more strict on rejecting pps_id changes
mmvideo : restore initial y value .
avformat / mux : Check that deinit is set before calling it
j2k : reorder multiplications to avoid interger overflows related to precincts
riff : don ' t overwrite bps from WAVEFORMATEX if EXTENSIBLE doesn ' t contain that data .
ffmpeg : add image size check to codec_get_buffer ()
examples / filtering_audio : fix frame leak .
check std tag size before reading .
ra144enc : fix two " may be used uninitialized in this function " warnings
avcodec / wmaprodec : check that there are input bits left in decode_subframe_length ()
avcodec / ratecontrol : silence " warning : expected_bits may be used uninitialized in this function "
avcodec / dpx : fix framerate
avformat / utils : Check cur_dts in update_initial_timestamps () more
applehttp : Change the variable for stream position in seconds into int64_t
avformat / oggparseopus : Check that granule pos is within the supported range
tests / api / api - codec - param - test : Do not directly access caps_internal
mpc8 : Check out of bound bands limit
motionpixels : clip VLC codes .
handle http error codes
smacker : Avoid integer overflow when allocating packets
doc / examples : Handle new types in avio_list_dir
matroskadec : check h in generic rm packet shuffler
avformat / avidec : set average frame rate from header
aac : Relax reserved_bit validation
cavsdec : check dimensions being valid .
wtv : Make WTV_SECTOR_BITS a 64 bit constant
binkaudio : prevent extradata overread
lavf / find_stream_info : make sure we decode at least one frame for DTS
Fix a small memory leak
avcodec / nvenc : Fix forcing constqp rc mode
avpacket : fix size check in packet_alloc
avcodec / mpegaudioenc_template : Fix integer overflow
avcodec / rangecoder : Fix range coder corner case handling
av_free () -> av_freep (), patch by Reimar Doffinger , see discussion in
lavd / xv : free graphics context
avformat / asfenc : Check for index_ptr allocation failure
af_amix : avoid spurious EAGAIN .
ac3enc : fix ' warning : block0 may be used uninitialized in this function '
avformat / concatdec : avoid NULL dereference when failed to open file .
matroskadec : fix NULL pointer dereference in webm_dash_manifest_read_header
h264 : return the consumed amountg in case of Q264
vp3 : Copy all 3 frames for thread updates .
libxvid : Clear extradata pointer when freeing it .
avcodec / rangecoder : Test for invalid corner case
avcodec / mjpegdec : sanity check bits
xmv : Fix integer overflow
avcodec / mpeg4videodec : update bug workaround related variables like they where before
incorrect memcpy size ; adobriyan @ gmail . com
initialize mmcpos , found by guenther
oops , bounds_check_with_label is already proto ' d in sys / disklabel . h
cast vararg sentinel value to pointer
Adds _PATH_RSH for rsh - using rcmd ()
bail out early , skip snprintf , if buflen is 0 ; ok cloder
plug a mem leak
Avoid an assertion failure in elf32_arm_size_dynamic_sections () when
rewrite construct that made GCC 2 . 8 do an implicit memset before libc
uhci : alloc can ' t fail , drop check .
trace : break circular dependency in event - internal . h
parallels : wrong call to bdrv_truncate
print meaningful error message in case of -- disable - vhost - net
vhost : check for vhost_ops before using .
qed : Refuse to create images on block devices
hw / intc / slavio_intctl : Avoid shifting left into sign bit
tests : Run the luks tests in test - crypto - block only if encryption is available
usb - serial : Fail instead of crash when chardev is missing
pseries : Don ' t allow TCE ( iommu ) tables to be registered with duplicate LIOBNs
qcow2 : Fix new L1 table size check ( CVE - 2014 - 0143 )
target / sh4 : fix TCG leak during gusa sequence
block : Detect multiplication overflow in bdrv_getlength
usb - bot : Drop not needed " allow_hotplug = 0 "
ide : fix halted IO segfault at reset
vvfat : initialize memory after allocating it
qemu - img : Fix insignificant memleak
exec : fix TB invalidation after breakpoint insertion / deletion
qdev : fix OVERFLOW_BEFORE_WIDEN
target - i386 : Make x86 mfence and lfence illegal without SSE2
sparc / leon3 . c : fix memory leak
s390x / skeys : Fix instance and class size
qemu - char : fix qemu_chr_fe_get_msgfd ()
hw / ppc / e500 . c : Fix memory leak
block : Fix anonymous BBs in blk_root_inactivate ()
cirrus : Fix host CPU blits
hostmem - file : plug a small leak
QemuOpts : check NULL input for qemu_opts_del
vhost : migration blocker only if shared log is used
bootdevice : add check in restore_boot_order ()
linux - user : fix broken cpu_copy ()
vmdk : Fix integer overflow in offset calculation
net : Avoid NULL function pointer dereference on cleanup
vmxnet3 : Do not fill stats if device is inactive
l2tpv3 : fix possible double free
target - s390x : Only access allocated storage keys
fix : unrealize virtio device if we fail to hotplug it
hw / xen_disk : aio_inflight not released in handling ioreq when nr_segments == 0
s390x : fix KVM target
loader : fix potential memory leak
target - i386 : fix protection bits in the TLB for SMEP
Fix a crash with monitor input arriving before readline_start has been called
qemu - img : Plug memory leak on block option help error path
e1000e : Fix PBACLR implementation
ide : remove undefined behavior in ide - test
ppc / spapr_drc : fix memory leak
block / raw - posix : Don ' t think / dev / fd /< NN > is a floppy drive .
vl : new runstate transition : RUN_STATE_GUEST_PANICKED -> RUN_STATE_FINISH_MIGRATE
hw / arm / highbank : Fix resource leak and wrong image loading
lm32 : avoid buffer overrun
target - mips : fix EntryHi . EHINV being cleared on TLB exception
linux - user / sh4 : fix incorrect memory write
dma : Fix stupid typo / thinko
macio : set res_count value to 0 after non - block ATAPI DMA transfers
exec : Fix bounce buffer allocation in address_space_map ()
sdhci : don ' t raise a command index error for an unexpected response
qcow2 : Avoid integer overflow in get_refcount ( CVE - 2014 - 0143 )
hw / ptimer : Do not artificially limit timers when using icount
block / qcow2 - bitmap : fix use of uninitialized pointer
vhost - user : unregister slave req handler at cleanup time
io : fix mem leak in websock error path
qom : Fix crash with qom - list and link properties
qcow2 : fix null pointer dereference
virtio : Plug memleak by freeing vdev
hw / 9pfs : Be robust against paths without FS_IOC_GETVERSION
darwin - user : Remove unneeded null pointer check
linux - user : make bogus negative iovec lengths fail EINVAL
sparc32 fix np dereference in do_unassigned_access
vhost - user : disable chardev handlers on close
pci_bridge : use parent bus ' s address space
block : Simplify usb_msd_initfn () test for " can read bdrv key "
block / vmdk : Fix the endian problem of buf_len and lba
qemu - socket : don ' t leak opts on error
virtio : Remove malloc failure checks ( Jan Kiszka )
Avoid a build warning .
qcow2 : Point to amend function in check
vnc : fix segfault
hw / arm : Mark the " fsl , imx31 " device with user_creatable = false
clang : Fix warning reg . expansion to ' defined '
vdi : Fix memory leak
tests : fix e1000 - test leak
qcow2 : Fix corruption after error in update_refcount
virtio - net : don ' t touch virtqueue if vm is stopped
isapc : Fix irq routing
hw / openrisc : Avoid undefined shift in openrisc_pic_cpu_handler ()
qcow2 : Remove abort on free_clusters failure
tcg - sparc : Dump illegal opode contents
hmp : fix qemu crash due to ioapic state dump w / split irqchip
exec - all . h : Make MAX_OP_PER_INSTR large enough for target - arm ' s uses
fsdev : fix memory leak in main ()
qxl : Slot sanity check in qxl_phys2virt () is off by one , fix
ivshmem : remove redundant assignment , fix crash with msi = off
block / qcow2 : Add missing GCC_FMT_ATTR to function report_unsupported ()
block / mirror : fix broken sparseness detection
block : Fix harmless off - by - one in bdrv_aligned_preadv ()
msix : implement pba write ( but read - only )
hw / display / exynos4210_fimd : Fix bit - swapping code
watchdog : fix deadlock with - watchdog - action pause
scsi - disk : change disk serial length from 20 to 36
rdma : memory leak RDMAContext :: host
sdl : remove NULL check , g_malloc0 can ' t fail
tcg / ppc64 : Fix zero extension code generation bug for ppc64 host
usb : fix use after free
vnc : fix a qio - channel leak
hw / arm / digic : Mark device with user_creatable = false
dma - helpers : Initialize DMAAIOCB in_cancel flag
pcihp : fix possible array out of bounds
target - s390x : fix setcc in TCG mode
pseries : Check we have a chardev in spapr_vty_init ()
tests : fix bios - tables - test leak
hmp : migrate command ( without - d ) now blocks correctly
block : Fix bdrv_is_allocated () return value
linux - user : fix do_rt_sigreturn on m68k linux userspace emulation
linux - user : Handle brk () attempts with very large sizes
vvfat : Fix segfault on write to read - only disk
linux - user / signal . c : PPC : Silence clang uninitialized - use warning
usb / ohci : Fix crash with when specifying too many num - ports
ppc : avoid buffer overrun : use pstrcpy , not strncpy
keep the PID file locked for the lifetime of the process
net : avoid infinite loop when receiving packets ( CVE - 2015 - 5278 )
spapr_rng : disable hotpluggability
qxl : switch display on entering VGA
iohandler : Use bool for boolean struct member and remove holes
qemu - io : Fix memory leak in ' aio_write - z '
char : remove use - after - free on win - stdio
microblaze : boot : Don ' t hack the elf entry point
usb - ohci : Fix memory leak for ohci timer
linux - user / signal . c : Correct error path for AArch64 do_rt_sigreturn
qemu - ga : ga_open_pidfile (): use qemu_open ()
vhost : remove an incorrect assert
qcow2 : Prevent numerical overflow
qemu - thread : do not use PTHREAD_MUTEX_ERRORCHECK
s390 / sclpconsole : prevent char layer callback during initialization
virtio - net : count VIRTIO_NET_F_MAC when calculating config_len
i8254 : fix out - of - bounds memory access in pit_ioport_read ()
replay : exception replay fix
vhost - user : fix watcher need be removed when vhost - user hotplug
hw / display / cg3 . c : Fix memory leak
lm32 : fix build breakage due to uninitialized variable ' r '
e500 : fix memory leak
device_tree : Fix potential memory leak
qcow2 : fix possible corruption when reading multiple clusters
tpm_tis : Return TPM_VERSION_UNSPEC in case of BE failure
thread - pool : add missing qemu_bh_cancel in completion function
target - arm : Forbid mode switch to Mon from Secure EL1
vmxnet3 : validate interrupt indices read on migration
qdev - monitor : Avoid device_add crashing on non - device driver name
pci : Only unmap bus_master_enabled_region if was added previously
s390 : Fix memory leak
qemu - ga : don ' t leak a file descriptor upon failed lockf
qemu - char : do not leak QemuMutex when freeing a character device
qcow2 : Correct snapshots size for overlap check
sysbus : fix memory leak
s390x / css : check ccw address validity
cirrus : don ' t overflow CirrusVGAState -> cirrus_bltbuf
ipmi_bmc_sim : Fix off by one in check .
loader : fix a file descriptor leak
vnc : fix coverity warning
vnc : fix qemu crash when not configure vnc option
s390x : s390_cpu_get_phys_page_debug has to return - 1
vnc : clear vs -> tlscreds after unparenting it
esp : fix coding standards
nand : fix address overflow
tap : clear vhost_net backend on cleanup
target - s390x : Add missing tcg_temp_free_i64 () in disas_s390_insn (), opc == 0x8e
rng random backend : check for - EAGAIN errors on read
tests : fix eepro100 - test leak
s390x : reduce TARGET_PHYS_ADDR_SPACE_BITS to 62
rdma : Fix qemu crash when IPv6 address is used for migration
target - ppc : fix a TCG local variable creation
block / vvfat : Plug memory leak in enable_write_target ()
rdma : bugfix : ram_control_save_page ()
vhost / scsi : call vhost_dev_cleanup () at unrealize () time
qcow2 - bitmap : fix bitmap_free
xen - platform : Ensure xen is enabled when initializing
ui / spice - display . c : add missing initialization for valgrind
qdev : don ' t crash on unset drive properties .
spapr : Fix undefined behaviour in spapr_tce_reset ()
fix QemuOpts leak
block / parallels : fix access to not initialized memory in catalog_bitmap
cirrus : fix off - by - one in cirrus_bitblt_rop_bkwd_transp_ * _16
suppress a couple of spurious warnings in scsi - generic . c
scsi : Make device scsi - disk reject / dev / sg *
vnc : fix crash when vnc_server_info_get has an error
tests : fix q35 - test leaks
tci : don ' t write zero for reloc in tci_out_label
s390x : Limit s390 - ccw machines to 248 CPUs
slirp : don ' t use " smb ports = 0 " option
qcow2 : Avoid overflow in alloc_clusters_noref ()
hw / scsi - bus . c : Fix use of uninitialised variable
hid : Reset kbd modifiers on reset
hw / omap_gpio : Fix infinite recursion when doing 8 / 16 bit reads
qbus : fix memory leak in qbus_free ()
tests : fix check - qobject leak
rtl8139 : honor RxOverflow flag in can_receive method
vpc : Handle failure for potentially large allocations
pseries : Fix debug message for out - of - bounds address in H_PUT_TCE
pci : deassert intx when pci device unrealize
[ virtio - 9p ] open should not return EBADF
crypto : fix leak in ivgen essiv init
qcow2 : Fix leaks in dirty images
libqos : Correct memory leak
qapi : Avoid output visitor crashing if it encounters a NULL value
qemu - sockets : Plug fd leak on unix_connect_opts () error path
hw / 9pfs / virtio - 9p - posix - acl : Fix out - of - bounds access
target / arm : fix TCG temp leak in aarch64 rev16
qemu - img bench : Fix uninitialised writethrough mode
ide : Fix off - by - one error in array index check
rtl8139 : Do not consume the packet during overflow in standard mode .
target - i386 : ROR r8 / r16 imm instruction fix
ui : Make qemu_default_pixman_format () return 0 on unsupported formats
slirp : fork_exec (): Don ' t close () a negative number in fork_exec ()
char : Fix socket with " type ": " vsock " address
sparc64 : fix TT_WOTHER value
target / openrisc : Fix writes to interrupt mask register
target - mips : Correct the handling of register # 72 on writes
acl : acl_add can ' t insert before last list element , fix
VMXNET3 : initialize rx_ridx to eliminate compile warning
libqos : remove some leaks
block : fix dangling bs -> explicit_options in block . c
pci : Fix crash with illegal "- net nic , model = xxx " option
virtio - 9p : Fix syntax error in debug code
scsi : initialise info object with appropriate size
exec - all . h : Use stl_p to avoid undefined behaviour patching x86 jumps
s390x / mmu : Fix the exception codes for illegal table entries
target - arm : Stage 2 permission fault was fixed in AArch32 state
usb : fix usb - net segfault
blockdev : unset inappropriate flags when changing medium
vhost : assert the log was cleaned up
hw / 9pfs : Fix potential memory leak and avoid reuse of freed memory
hw / lan9118 . c : Add missing ' break ' to fix buffer overrun
Qcow2 corruption fix , by Juergen Keil .
do_snapshot_blkdev () error on missing snapshot_file argument
ide : Check validity of logical block size
vnc : fix qemu crash because of SIGSEGV
target - alpha : fix decoding of CVTST / S
qemu / slirp : Fix SMB security configuration on newer samba versions
ahci : fix buffer overrun on invalid state load
use new helper name
acpi - build : fix misaligned access
xen - pt : fix Out - of - bounds read
tpm : Fix write to file descriptor function
ps2 : reset queue in ps2_reset_keyboard
exec : fix breakpoint_invalidate when pc may not be translated
linux - user : assert that target_mprotect cannot fail
xen_disk : fix possible null - ptr dereference
ahci : free irqs array
target / m68k : fix TCG variable double free
x86 : ioapic : fix fail migration when irqchip = split
qcow2 : Prevent allocating compressed clusters at offset 0
tests : fix ipmi - bt - test leak
pc_sysfw : Plug memory leak on pc_fw_add_pflash_drv () error path
x86 / cpuid : Tighten parsing of tsc_freq = FREQ
target - ppc : Fix Temporary Variable Leak in bctar
qdev - monitor : Fix crash when device_add is called with abstract driver
qdev : Fix crash by validating the object type
fix memory leak in aio_write_f
load_image_targphys () should enforce the max size
qapi / block : Add " fatal " to BLOCK_IMAGE_CORRUPTED
qcow2 : Fix error handling in qcow2_alloc_cluster_offset
usb - storage : fix possible memory leak and missing error message
qcow2 : Put cache reference in error case
qapi : Add corrupt field to ImageInfoSpecificQCow2
virtio : zero vq -> inuse in virtio_reset ()
target - s390x : fix CC computation for LOAD POSITIVE instructions
gtk : skip keyboard grab when hover autograb is active
slirp : tftp , copy sockaddr_size
target - i386 : Fix x86_cpuid_set_model_id ()
hw / arm / allwinner - a10 : Mark the allwinner - a10 device with user_creatable = false
s390x / cpu : use g_new0
target - arm : Make arm_current_el () return sensible values for M profile
tcg : Fix compiler error ( comparison of unsigned expression )
bitops : fix test_and_change_bit ()
dt : temporarily disable subtree creation failure check
hw / arm / aspeed_soc : Mark devices as user_creatable = false
exec . c : Fix off - by - one error in register_subpage
qcow2 : Fix header extension size check
lm4549 : Fix buffer overflow
char / serial : fix copy & paste error ( fifo8_is_full vs empty )
s390x / sclp : mark sclp - cpu - hotplug as non - usercreatable
cryptodev : add xts ( aes ) support
qxl : locking fix
Add a path length check to prevent heap overflow ( Eric Milliken ).
qemu - thread : fix qemu_event without futexes
json - streamer : Don ' t leak tokens on incomplete parse
virtio : error out if guest exceeds virtqueue size
ARM : ACPI : fix the AML ID format for CPU devices
scsi - block : fix direction of BYTCHK test for VERIFY commands
intc : i8259 : Convert Array allocation to g_new0
kvm : Switch to unlocked MMIO
vhost - user : check qemu_chr_fe_set_msgfds () return value
stream : fix crash in stream_start () when block_job_create () fails
scsi : Specify the xfer direction for UNMAP and ATA_PASSTHROUGH commands
io : fix qio_channel_socket_accept err handling
ppc / xics : An ICS with offset 0 is assumed to be uninitialized
spapr : fix off - by - one error in spapr_ovec_populate_dt ()
block : Fix memory leaks in bdrv_co_do_pwritev ()
block : better error message for read only format name
iscsi : Handle failure for potentially large allocations
qdev : Use qdev_get_device_class () for - device < type >, help
scsi : Fix scsi_bus_legacy_add_drive () scsi - generic with serial
spapr_drc : Expose ' null ' in qom - get when there is no fdt
xhci : fix numintrs sanity checks
console : fix - vga none - sdl crash
i386 : define the AMD ' amd - ssbd ' CPUID feature bit
QMP : Check " arguments " member ' s type
AHCI : Fix port reset race
hw / pl011 . c : Avoid crash on read when no chr backend present
target - ppc / fpu_helper : Fix efscmp * instructions handling
qemu - char : initialize chr_write_lock
usb - storage : Drop not needed " allow_hotplug = 0 "
pci - assign : Fix potential read beyond buffer on - EBUSY
i6300esb : fix timer overflow
spice : fix memory leak
powerpc / kvm : Fix a uninitialized bug ( Liu Yu )
ui : fix dcl unregister
tests : fix virtio - 9p - test leaks
pcnet : fix rx buffer overflow ( CVE - 2015 - 7512 )
crypto : assert that qcrypto_hash_digest_len is in range
target - arm : resource leak fixes for iwmmxt disassemble
block : clean up bdrv_drain_all () throttling comments
ide : Remove unneeded null pointer check
qcow2 : Fix memory leak in COW error path
qjson : Apply nesting limit more sanely
kvm : de - register mem region for MMIO ( Glauber Costa )
pci : fix pci_info_device ().
target - i386 : Check for host features before filter_features_for_kvm ()
kvm : add error message for when SMP is requested
hmp : fix memory leak at hmp_info_block_jobs ()
input : Add trace event for empty keyboard queue
mips_mipssim : fix use - after - free for filename
hw / vfio / platform : replace g_malloc0_n by g_new0
vhost : fix mem_sections memory corruption
qcow2 : Check backing_file_offset ( CVE - 2014 - 0144 )
dma : xlnx - zynq - devcfg : Fix up XLNX_ZYNQ_DEVCFG_R_MAX
qom : fix leak for objects created with - object
qcow2 : Assert that the crypto header does not overlap other metadata
iothread : Make iothread_stop () idempotent
vhost - scsi : add missing virtqueue_size parameter
throttle : Set always an average value when setting a maximum value
slirp : Check qemu_socket () return value in udp_listen ()
kvm : Fix potential resource leak ( missing fclose )
linux - user : Fix invalid TARGET_ABI_BITS usage on ppc hosts
cadence_uart : bounds check write offset
cadence_gem : avoid stack - writing buffer - overrun
acpi : avoid potential uninitialized access to cpu_hp_io_base
target / arm : Fix 32 - bit address truncation
gtk : fix possible memory leak about local_err
fdc - test : insert media before fuzzing registers
vhost : check region type before casting
exec : avoid possible overwriting of mmaped area in qemu_ram_remap
error : Copy location information in error_copy ()
qemu - char : fix warning ' res ' may be used uninitialized
iothread : check iothread -> ctx before aio_context_unref to avoid assertion
qemu - ga : Plug fd leak on ga_channel_open () error paths
qdev : Free opts on failed do_device_add
xen_pt : Present the size of 64 bit BARs correctly
qdev : Check if unplug handler exists before calling it
usb : Fix usb_ep_find_packet_by_id
qemu - img : Fix memory leak
qdev : Fix qdev_try_create () semantics
vmdk : Fix use of uninitialised value
gdbstub : do not restart crashed guest
armv7m : R14 should reset to 0xffffffff
vl : fix memory leak spotted by valgrind
qemu : msix nit : clear msix_entries_nr on error
crypto : fix mistaken setting of Error in success code path
qlist : Make conversion from QObject * accept null
apic : kvm - apic : Fix crash due to access to freed memory region
target - i386 / kvm . c : Code formatting changes
qcow2 : Free allocated snapshot table on error
vl : Add failure check for SetEvent
Check the returned audio_buf_info fields
qcow2 : Fix refcount table size calculation
xenfb : use the correct condition to avoid excessive looping
block : Fix serial number assignment
hw / vmmouse . c : Disable vmmouse after reboot
virtio - 9p - proxy : Fix sockfd leak
dcbz fix ( Jocelyn Mayer )
check - qdict : Fix possible crash
Initialize a variable in all cases
virtio : optimize virtio_access_is_big_endian () for little - endian targets
hw / display / tcx . c : Fix memory leak
spapr : support only ELF kernel images
pc - dimm : don ' t assert if pc - dimm alignment != hotpluggable mem range size
block / vvfat : Plug memory leak in read_directory ()
console : Fix warning from clang ( and potential crash )
qemu - io : fix a memory leak
tcg : correctly mark dead inputs for mov with a constant
qemu - timer . c : Trim list of included headers
target - i386 : Fix apic object leak when CPU is deleted
blkverify : Fix leak of opts in blkverify_open
xen_disk : mark ioreq as mapped before unmapping in error case
s390x / tcg : handle WAIT PSWs during interrupt injection
timer : stm32f2xx_timer : add check for prescaler value
qom : Fix memory leak in object_property_set_link ()
spapr_pci : parent the MSI memory region to the PHB
ppc64 : fix compressed dump with pseries kernel
vnc : fix incorrect checking condition when updating client
cpu_physical_memory_write_rom () needs to do TB invalidates
ppc405 : Fix memory leak
throttle : Remove block from group on hot - unplug
qga : fix uninitialized value warning for win32
spapr : initialize local Error pointer
block / drive - mirror : Check for NULL backing_hd
slirp : fix structure initialization in tcp_listen ()
mptsas : fix memory leak
crypto : fix stack - buffer - overflow error
qemu - option : Make qemu_opts_foreach () accumulate return values
block / nbd : Fix the leaked visitor
qga : fix false negative argument passing
gtk : fix uninitialized temporary VirtualConsole
win32 - aio : Fix memory leak
s390x / css : start with cleared cstat / dstat
spapr_vscsi : Set uninitialized variable
pci : fix double free of romfile property
scsi : Add assertion for use - after - free errors
parallels : fix default options parsing
sheepdog : fix NULL dereference in sd_create
dirty - bitmap : Report BlockDirtyInfo . count in bytes , as documented
audio : Don ' t free hw resources until after hw backend is stopped
blockdev : Plug memory leak in drive_uninit ()
sheepdog : fix resource leak with sd_snapshot_create
hw / arm : Mark the " fsl , imx25 " device with user_creatable = false
vhost - net : do not crash if backend is not present
hw / mips_malta : remove redundant irq and clock init
fixed memory leak
bitmap . h : Don ' t include qemu - common . h
disas / i386 : Avoid NULL pointer dereference in error case
qemu - ga : Plug file descriptor leak on ga_open_pidfile () error path
KVM : Use return value for error print
qga : fix off - by - one length check
vl : Tighten parsing of - numa ' s parameter mem
console : add same surface replace pre - condition
pci : memory leak of PCIDevice :: rom_file
vmxnet3 : validate queues configuration read on migration
ide : only constrain read / write requests to drive size , not other types
tests : fix leak in test - string - input - visitor
spice : abort on invalid streaming cmdline params
ide : refuse WIN_READ_NATIVE_MAX on empty device
ui / curses . c : Ensure we don ' t read off the end of curses2qemu array
Add a pc - 0 . 11 machine type and make the pc type an alias
target - mips : save cpu state before calling MSA load and store helpers
target - alpha : Fix compiler warning for gcc - 4 . 3 ( and older )
usb - smartcard - reader : Properly NAK interrupt eps when we ' ve no events
valgrind : avoid false positives in KVM_GET_DIRTY_LOG ioctl
linux - user : Fix error handling in flatload . c target_pread ()
hw / openrisc : Avoid using uninitialised variable ' entry '
qemu - sockets : use qapi_free_SocketAddress in cleanup
ui : sdl2 : Release grab before opening console window
ehci : fix overflow in frame timer code
DOS 6 . 22 fix ( Johannes Schindelin )
pci : Use of qemu_put_ram_ptr in pci_add_option_rom .
qdev - monitor : fix segmentation fault on qdev_device_help ()
qapi : Fix c_name () munging
linux - user / syscall . c : lock_iovec : unlock vec [ i ] in failure processing code block
json - parser : add handling for NULL token list
cpus : initialize cpu -> memory_dispatch
block : Fix multiwrite memory leak in error case
linux - user : fix file descriptor leaks
input : Decrement queue count on kbd delay
qemu - img : add check for zero - length job len
hmp : fix crash in ' info block - n - v '
rtc - test : skip year - 2038 overflow check in case time_t is 32bit only
sdl : Do not make full screen mode resizable
target - i386 : Fix mulx for identical target regs
linux - user : zero fstat buffer to initialize nsec fields
slirp : fix CVE 2007 - 5729
spapr : sanity check size of the CAS buffer
qxl : clear irq on reset
ccid - card - emul : do not crash if backend is not provided
kvm / ioapic : dump real object instead of a fake one
linux - user : remove ifdef __USER_MISC
qemu / iovec : Don ' t assert if sbytes is zero
virtio - net : fix guest - triggerable buffer overrun
vfio / ccw : allocate irq info with the right size
fix race between timer firing vs . alarm_timer -> pending = 0
qcow2 : Fix L1 allocation size in qcow2_snapshot_load_tmp () ( CVE - 2014 - 0145 )
qemu - ga : for w32 , fix leaked handle ov . hEvent in ga_channel_write ()
rtl8139 : check IP Total Length field ( CVE - 2015 - 5165 )
cadence_uart : Fix buffer overflow
qxl : Fix new function name for spice - server library
qcow2 : Remove dead check on ! ret
qemu - common : add QEMU_ALIGN_DOWN () and QEMU_ALIGN_UP () macros
qcow2 : fix offset overflow in qcow2_alloc_clusters_at ()
exit if - drive specified is invalid instead of ignoring the " wrong " - drive
usb : fix up post load checks
target - ppc : fix nego and subf * o instructions
hw / arm / virt : Fix corruption due to double free
block : Fix bdrv_all_delete_snapshot () error handling
kvm : Add kvm system event crash handler
s390 : fix reset hypercall to reset the status
usb : sanity check setup_index + setup_len in post_load
xhci : fix overflow in usb_xhci_post_load
cpu : don ' t allow negative core id
linux - aio : Fix laio resource leak
qapi / string - output - visitor : fix human output
qemu / migration : fix the double free problem on from_src_file
hw / arm / virt : Check that the CPU realize method succeeded
qemu - timer : fix off - by - one
tests : fix rsp leak in postcopy - test
pc - dimm : error out if memory hotplug is not enabled
ui : fix crash with sendkey and raw key numbers
ivshmem : fix memory_region_del_eventfd assertion failure
s390x : silence warning from GCC on uninitialized values
qemu - char : qemu_chr_open_fd () can ' t fail , don ' t check
mirror : Fix resource leak when bdrv_getlength fails
hw / core / loader : Fix possible crash in rom_copy ()
vnc : fix segfault in vnc_display_pw_expire ()
e1000e : correctly tear down MSI - X memory regions
acpi : add dummy write function for acpi timer
slirp : fix guestfwd id
audio : fix WAVState leak
pci - devfn : check that device / slot number is within range
target - alpha : fix uninitialized variable
pseries : Correct panic behaviour for pseries machine type
flush TLBs at cpu reset
qdev : fix use - after - free in the error path of qdev_init_nofail
tests : fix memory leak in virtio - scsi - test
container : don ' t leak container reference
pci : Clean up error checking in pci_add_capability ()
qcow2 : fix condition in is_zero_cluster
hw / ppc / spapr_iommu : Fix crash when removing the " spapr - tce - table " device
vvfat : Fix compilation with DEBUG defined
page_cache : fix memory leak
usb - ehci : drop assert ()
vga : fix invalid read after free
mempath prefault : fix off - by - one error
qcow2 : Fix sector calculation in qcow2_measure ()
vhost - user - bridge : fix resume regression ( since 2 . 9 )
arm_gicv3 : Fix broken logic in ELRSR calculation
xio3130_downstream : Report error if pcie_chassis_add_slot () failed
usb : xhci : fix memory leak in usb_xhci_exit
linux - aio : Fix laio_submit error handling
virtio - net : fix buffer overflow on invalid state load
usb : free leaking path
hw / usb / bus : Remove bad object_unparent () from usb_try_create_simple ()
qemu - socket : set passed fd non - blocking in socket_connect ()
isapc : fix segfault .
qcow2 : Don ' t open images with header . refcount_table_clusters == 0
hw / intc / arm_gicv3_common : Give no - migration - shift - bug subsection a needed function
pci : fix corrupted pci conf index register by unaligned write
block / gluster . c : Handle qdict_array_entries () failure
input : don ' t queue delay if paused
hw / acpi / aml - build : Fix memory leak
virtio : correctly initialize vm_running
vga : check the validation of memory addr when draw text
qcow2 : don ' t leak buffer for unexpected qcow_version in header
qcow2_check : Mark image consistent
usb : ehci : fix memory leak in ehci_init_transfer
target - arm / translate - a64 : Fix wrong mmu_idx usage for LDT / STT
target - i386 : Don ' t strdup () alias property name
qom : Fix memory leak in function container_get
block : Assert validity of BdrvActionOps
vnc : Fix fatal crash with vnc reverse mode
target - i386 : fix Coverity complaints about overflows
sheepdog : don ' t leak socket file descriptor upon connection failure
target - arm : Correct " preferred return address " for cpreg access exceptions
hw / ppc / spapr_rtc : Mark the RTC device with user_creatable = false
raw_bsd : don ' t check size alignment when only offset is set
hw / 9pfs : Fix crash when mounting with synthfs
aio - win32 : fix uninitialized use of have_select_revents
mirror : Handle failure for potentially large allocations
qcow2 : Check min_size in qcow2_grow_l1_table ()
ppc : Fix bug in handling of PAPR hypercall exits
qdev : Fix memory leak in function set_pci_devfn
usb - redir : fix use - after - free
target - i386 : Use 1UL for bit shift
kvm : Mark VCPU state dirty on creation
i386 ROR r8 / r16 instruction fix
vmdk : Fix big flat extent IO
block / mirror : check backing in bdrv_mirror_top_refresh_filename
vnc : don ' t crash getting server info if lsock is NULL
block / raw - posix : Open file descriptor O_RDWR to work around glibc posix_fallocate emulation issue .
slirp : udp : fix NULL pointer dereference because of uninitialized socket
pc_sysfw : Check for qemu_find_file () failure
vvfat : Fix potential buffer overflow
fix qemu - system - unicore32 crashing when calling without - kernel
pci - hotplug : initialize dinfo to NULL in pci_device_hot_add
target - ppc : KVMPPC_H_CAS fix cpu - version endianess
pc : memory : Validate alignment of maxram_size to page size
target / ppc : fix cpu_ov setting for 32 - bit
qapi : fix memory leak in bdrv_image_info_specific_dump
block / vpc : make checks on max table size a bit more lax
slirp : Fix memory leak on small incoming ipv4 packet
vfio - pci : Further fix BAR size overflow
qcow2 : Fix potential qemu - img check crash on 32 bit hosts
vt82c686 : avoid out - of - bounds read
qcow2 corruption : Fix alloc_cluster_link_l2 ( Kevin Wolf )
vmdk : Widen before shifting 32 bit header field
scsi - bus : Fix transfer length for VERIFY with BYTCHK = 11b
qemu - sockets : Fix potential memory leak
hw / ppc / spapr_cpu_core : Add a proper check for spapr machine
qdev - properties . h : Explicitly set the default value for arraylen properties
rtl8139 : check TCP Data Offset field ( CVE - 2015 - 5165 )
xen / pt : xen_host_pci_config_read returns - errno , not - 1 on failure
qcow2 : fix leak of Qcow2DiscardRegion in update_refcount_discard
tests : fix coroutine leak in / basic / entered
exec : fix ram_list dirty map optimization
usb - host : Initialize dev -> port the obviously safe way
hw / intc / xilinx_intc : Avoid shifting left into sign bit
hw / mips_malta : Fix YAMON API print routine
sheepdog : Fix crash in co_read_response ()
linux - user : avoid gcc array overrun warning for sparc
sheepdog : Fix misleading error messages in sd_snapshot_create ()
qemu - img : fix failed autotests
block : Catch integer overflow in bdrv_rw_co ()
fix build failure in nbd_read_reply_entry ()
usb - storage : fix NULL pointer dereference .
hw / misc / mmio_interface : Return after error_setg () to avoid crash
qxl : stop dirty loging when not in vga mode
s390 - ccw : Fix alignment for CCW1
vnc : avoid possible file handler leak
pseries : Fix array overrun bug in PCI code
qxl : simplify qxl_rom_size
qemu - char : fix infinite recursion connecting to monitor pty
s390x : fix cpu object referrence leak in s390x_new_cpu ()
pc_sysfw : Fix ISA BIOS init for ridiculously big flash
qcow2 : remove memory leak
sockets : avoid crash when cleaning up sockets for an invalid FD
rtl8139 : skip offload on short Ethernet / IP header ( CVE - 2015 - 5165 )
block : avoid useless checks on acb -> bh
spapr_cpu_core : drop reference on ICP object during CPU realization
mirror : make sectors_in_flight int64_t
kvmvapic : Disable if there is insufficient memory
xlnx - ep108 : Fix minimum RAM check
spapr : fix leak in h_client_architecture_support ()
e1000 : flush packets when link comes up
spapr : fix write - past - end - of - array error in cpu core device init code
tap - win32 : skip unexpected nodes during registry enumeration
QOM : Fail casts for unknown types
fsdev : fix virtfs - proxy - helper cwd
qemu - img : Fix crash in amend invocation
linux - user / signal . c : Don ' t pass sigaction uninitialised sa_flags
block / qcow . c : Fix memory leak in qcow_create ()
virtio : add check for descriptor ' s mapped address
block : vhdx - add error check
check for bs -> drv in bdrv_flush ( Christoph Hellwig )
target - arm : Reinsert missing return statement in ARM mode SRS decode
ide : ahci : unparent children buses before freeing their memory
tests / bios - tables - test : check the value returned by fopen ()
acpi : fix assert failure caused by commit 35c5a52d
target - ppc : fix left shift overflow in hpte_page_shift
hw / pci : fix crash on shpc error flow
qcow2 : Assert against snapshot name / ID overflow
io : avoid double - free when closing QIOChannelBuffer
PPC : KVM : Synchronize regs on CPU dump
spapr_vty : lookup should only return valid VTY objects
vl : Set errp to & error_abort on machine compat_props
qga - win32 : Fix memory leak of device information set
i386 : hvf : inject General Protection Fault when vmexit through vmcall
io : assert errors before asserting content in I / O test
qcow2 : Fix alloc_clusters_noref () overflow detection
vl : exit qemu on guest panic if - no - shutdown is not set
sd : sdhci : check data length during dma_memory_read
block / iscsi : fix memory corruption on iscsi resize
linux - user : don ' t crash with null name
slirp : fix leak
vmdk : fix buf leak in vmdk_parse_extents ()
hpet : Fix emulation of HPET_TN_SETVAL ( Jan Kiszka )
slirp : don ' t crash when tcp_sockclosed () is called with a NULL tp
qcow2 : Fix grow_refcount_table error handling
tcg - sparc : Fix small 32 - bit movi
target - microblaze : dec_barrel : Plug TCG temp leak
virtio - net : properly check the vhost status during status set
sd : do not add one sector to the disk size
vhost - user - test : fix predictable filename on tmpfs
qemu - img convert : Use cache = unsafe for output image
pci : move dereferencing of root only after verifying valid root pointer
fsdev : Fix potential memory leak
vvfat : fix out of bounds array_get usage
stdio_pclose : return - errno on error ( v3 )
target - tricore : Fix helper_msub64_q_ssov not reseting OVF bit
parallels : drop check that bdrv_truncate () is working
blockdev : reset werror / rerror on drive_del
vpc : fix return value check for blk_pwrite
arm : fix compile on bigendian host
qcow2 : Use sizeof (** refcount_table )
hw / arm_gic . c : Define . class_size in arm_gic_info TypeInfo
check malloc return for code in rabin2 . c
prevent a possible double - free
core / graph : free RGraphEdges when freeing long_edges list
add warning to zos command (# 8585 )
versioninfo : Fix SEGV on files with no info (. txt , ...)
take bits " wildcard " into account in r_bp_size (# 8994 )
fix oob in dex
Fixing the breakpoint callback signature . (# 10998 )
Fix a gcc warning
CID 1134732 Out - of - bounds access / libr / asm / arch / avr / format . c
fix hang in version info ELF
format / zimg : make sure there are enough bytes in the buffer
bin : Delete the list of objs instead of deleting current object
riscv : Return actual instruction length
Disable self :// in sandbox mode
bail out when no proxy was configured
varnishncsa : Don ' t output ESI subrequests
Remove a now pointless assert .
note a TODO
Bail out of CLI reads on signals
Added connection - specific conn_data pointer to struct mg_request_info
logging : use fchmod instead of chmod to avoid TOCTOU race
plugins / emperor_amqp : fix leak on error path
attempt to fix # 713
core / alarm : fix memory leak in uwsgi_alarm_thread_loop
added a security check in the http router
realpath on BSD is different from Linux / Solaris / OSX
fixed a bug in setrlimit
plugins / python : check uwsgi_connect return code in uwsgi_python_harakiri
fixed a bug with -- set
plugins / v8 : fix double free
threads must be implicit in PyPy
report socket name on invalid zeromq syntax
plugins / router_cache : fix memory leak in uwsgi_router_cache_store
plugins / python : fix tracebaker resource leaks on import error
fixed sigquit / sigint in hijacked mode
fixed goodbye_cruel_world in gevent mode
Added an additional check in some code to avoid dividing by zero ...
Attempt to appease travis
Units / Map : added an empty () function
Try to fix bug # 8885 ( negative gold amount gets changed while save / load )
idle controlled teams serialize as " human " controlled
updated about : Guillaume Duwelz - Rebert removed from developers
fixed bug where entering a chat message while the message is selected ...
Set the core_id preferences item at game launch .
Added dedicated fallback message if a replay is loaded from mp create
Fixed a missing brake in a switch case construct , spotted by Crab .
Update the GUI when using [ replace_schedule ]
Fixed huge cpu usage when the window is hidden .
fixed bug where the program could freeze up when networked
Initialize all members .
use a list for the standard message receiving sound
Initialize all members .
add game start notification
Initialize all members .
Fix : with -- new - widgets advanced preferences may not shrink on deselect
Add a getter for need_layout_ .
Initialize all members .
fixup ! Add DEPRECATED macro and remove uses of snprintf
fix ReadOK macro to avoid casting warnings
Fixed type of temporary variable in _TIFFCheckMalloc () as per bug
fix unchecked malloc result
* tools / tiff2pdf . c : avoid potential heap - based overflow in
lxc_spawn : initialize netpipe
cmd : Use ' void ' instead of empty parameter list
lxc_abstract_unix_connect : accomodate containers started before Oct 28
terminal : fix uninitialized variable in strerror () log message
sd - dhcp : check for udp packets
sd - network : fix memleak in dhcp6_option_parse_domainname (# 5114 )
[ PATCH ] fix spurious valgrind warning in udev .
sysusers : fix memleak (# 4443 )
systemctl : plug a leak upon create_symlink mismatch
install : explicitly return 0 on success
fix inotify to work not only once
seccomp : add proper help string for @ resources seccomp filter set
rfkill : Avoid error when state restore is disabled
terminal : discard async read () errors for evdev
bus : fix issue with reference counting
core : kill processes started due to the ExecReload = on timeout
hibernate - resume : refuse to run outside of an initrd
load - fragment : setting empty string to Nice = resets the previous assignments
tmpfiles : add missing OOM check
core / dbus - execute : actually set PassEnvironment = (# 7510 )
systemctl : fix check for template units
sd - event : fix theoretical leak on error in event_make_signal_data
shared : fix search_and_fopen with alternate roots
logind : fix printf format
systemctl : do not print header if no units will be listed
systemctl : Add missing "/" to files created by ' edit -- runtime '
basic : use for () loop instead of while ()
systemctl : never check inhibitors if - H or - M are used (# 3781 )
sd - dhcp - client : fix busy loop reading zero sized DHCP UDP packets . (# 3299 )
path - util : plug leak
bus - util : fix error handling
libudev : monitor - export MAJOR / MINOR only if available
fstab - generator : fix trivial leak
util : make sure we don ' t clobber errno in error path
cgroup : the root cgroup is always populated
locale : make sure that l is freed
terminal / subterm : use usec_t instead of " unsigned long "
udev : properly detect reference to unexisting part of PROGRAM ' s result
machinectl : make sure we are not reading an unitialized variable
cgroup : properly determine cgroups zombie processes belong to
switch - root : not only mkdir the parents but the target dir
pam : skip DBUS_SESSION_BUS_ADDRESS export when kdbus is not active
util : be a bit safer in path_is_safe ()
core / mount : skip incomplete mountinfo entries
journalctl : don ' t choke on entries with no MESSAGE = field
json : avoid cleanup of unitialized variable
sd - bus : correct size calculation in DBus fd receive
journald : fixed assertion failure when system journal rotation fails (# 9893 )
random - util : we are fine if ints are 16 bytes actually
core / manager : Make sure jobs statistics are not double after daemon - reload
bus_open leak sd_event_source when udevadm trigger 。
resolved : bugfix of null pointer p -> question dereferencing (# 6020 )
calendarspec : free memory when parsing fails (# 4890 )
don ' t choke if there are no sockets for a service
sd - dhcp - client : readd deleted error check
libudev : make valgrind happy
sd - resolve : fix check for packet size
usb_id : MassStorage SubClass 6 is " scsi " not " disk "
readd early log level check for performance reasons .
saEvtEventAttributesSet needs to check that the priority value passed in
set serialize lock to proper function .
Stop the totem stats updater timer during shutdown to prevent references to
cleanup valgrind warnings .
totemcrypto : fix hmac key initialization
totemsrp : Enhance mcast failure detection
libdocument : return 96 DPI if unable to determine true value
Prevent jumps to last page when autoscrolling
[ presentation ] Fix transition of pages with duration = 0
libview : Add check for pageless documents to ev_view_accessible_focus_changed
libview : Do not schedule a redraw when selecting all text
app : don ' t leak the GtkTextBuffers of Gegl multiline text properties
app : skip keyboard devices so the device list becomes reasonable
libgimpconfig : support $( gimp_installation_dir ) in config paths
app : make LCH lightness select - criterion faster
plug memory leak : Fixes # 305990
Bug 791514 - Cannot export to webp file .
Make the preview column the expander column
app : silence a harmless critical warning
libgimp : disable tile command range check in plug - in tile backend
Bug 595605 - memory leaks in plug - ins / common / curve - bend . c
plug - ins : Use string literal as format in metadata plug - in
plug - ins : fix memory leak in edge - neon
app : initialize variable in gimp_histogram_view_update_bins ()
app : finish porting GdkScreen / monitor_number to GdkMonitor
app : set an icon on the floating selection filter
Bug 686379 : In the JPEG save dialog , checking / unchecking XMP ...
app : typo : make string translatable
Bug 663582 - Brush outline is too slow
app : fix rectangle select tool cursor after committing / halting
app : add a tooltip to the color temperature preset menu button
app : avoid id . matrix short - circuit in trans . tool when ...
[ ot ] Ensure layout in hb_ot_layout_table_get_lookup_count ()
[ ot - font ] Fix hmtx wrong table length check
Bug 528272 – " Error loading GPOS table 5503 " when using katakana
make it compile with - ansi - pedantic
[ t : 5131 ] closes # 5131 .
Tabname defaults to the table name for MONGO tables
branches / zip : btr_search_guess_on_hash (): Do not call buf_block_get_frame (),
Remove the temporary file created by mysql_upgrade
branches / zip : lock_update_discard (): Fix a bug introduced in r949 :
Fixed a typo , which prevented non - valgrind build .
Whoops , freed something not malloced
Fixed issue with retrying autocommitted transactions . We might need to
branches / zip : Restore the transaction commit that was removed in r1583 .
initialize mysql -> charset in mysql_init
branches / zip : buf_page_init_low (): Initialize also flush_type .
sync0arr . c : Fix compilation error on some systems by introducing all
IB : production compilation fix
branches / zip : row_merge_create_temporary_table (): On error , free the
branches / zip : row_sel_sec_rec_is_for_clust_rec (): Augment the bug fix in r918 :
branches / zip : ha_innobase :: add_index (): Check for ! innodb_table .
sync0arr . c : Fix compilation error on some systems by introducing all
followup to SET PASSWORD fix
Added missing prototypes to mysys / my_bitmap . h
refs # 5560 , fix bug
allocate an array of pointers to ctpair ' s rather than an
refs # 5133 , fix compilation error
branches / zip : buf_LRU_block_remove_hashed_page (): Add some debug assertions .
trx_undo_parse_page_header (): Silence a bogus warning .
[ t : 4472 ], add table lock for truncate
connect zip bug fix
branches / zip : Check for autoinc next value overflow explicitly when offset
fix a build problem . addresses # 1396
Applying InnoDB snashot 5 . 0 - ss5406 , part 1 . Fixes BUG # 38479
Merge a patch from MySQL AB ( Mats Kindal ):
branches / zip : btr_cur_optimistic_insert (): If btr_page_reorganize ()
[ t : 5131 ] closes # 5131 .
Applying InnoDB snapshot
Fixed bug in sub select after last merge
fixed picture in comment .
fixed BINLOG crashes in case of badly formed statement ( Bug # 17457 )
correct fix for the bug # 6151
fix for bug # 212
branches / innodb +: row_purge_remove_sec_if_poss_low (): Remove a suspicious
[ trunk ] Import commit 8ec8321df613986e8642e2975f7182072eb4be62 from ghostpdl
src / bin / jpwl / convert . c : fix memleak ( fix suggested by maddin200 , # 631 )
[ trunk ] Fix Heap - based buffer - overflow when decoding openjpeg image
r15723 @ tombo : nickm | 2007 - 12 - 26 19 : 25 : 51 - 0500
fix duplicate dns_cancel_pending_resolve reported by Giorgos Pallas
Oct 19 13 : 27 : 24 . 391 [ info ] router_reload_networkstatus (): Skipping cached - status file with unexpected name "."
Add a missing return - 1
r8682 @ Kushana : nickm | 2006 - 08 - 29 17 : 58 : 59 - 0400
Generate our x509 certificates using sha256 , not sha1 .
r11753 @ Kushana : nickm | 2006 - 12 - 29 11 : 30 : 28 - 0500
fix memory leak introduced in 26e89742
fix a memory leak
take back half of my bugfix . comparing times sucks .
Bridges are always dirs
fix a seg fault with exitredirect
Treat a bad tor_spawn_background () as a BUG ().
r7341 @ Kushana : nickm | 2006 - 08 - 11 00 : 51 : 05 - 0700
Harmless : Z_OK is not an acceptable answer to Z_FINISH .
fix ECP curve BER decode
fix jsvGetString regression
fix precedence of ' void 0 '
wow - 2016 now
fix wifi restore ssidAP
replace signed with unsigned counter to avoid warning when compiling with
svn merge - r16071 : 16073 https :// eris . llnl . gov / svn / slurm / branches / slurm - 1 . 3
Initialize the entire launch structure .
Added missing unlock for pre - mature return from log_msg . - Jette
change the pthread locking to eliminate a race condition
move slurm_crypto_fini () to prevent memory leak on shutdown
fix for checking valid qos
removed debug message
fixed last patch
check for NULL pointer
fix for qos char list
Prevent zero divide
Decrease the time of batch start time no more than DEFAULT_MSG_TIMEOUT .
fix uninitialized text outline width in error image
fixed small leak in msRemoveStyle
Fixed bug 769 , duplicate error with TILEITEM .
Added the declaration for freeStyle to eliminate the unresolved externals .
don ' t free any static memory
Set the WSQ TLS key to inited after initing it .
[ runtime ] Speed - up MERP timeout
[ sgen / mono ] Don ' t leak stack info struct
[ llvm ] Fix 32 bit platform support which got broken after c678533842f10356968c203ed8fa43e86a406c6e .
tasklets . c : update used stack size
[ profiler ] Use thread_exited profiler event for recording thread end events .
Fix a bug introduced by the previous change to mono_jit_walk_stack_from_ctx_in_thread ().
[ runtime ] Fix minor leak
[ sgen ] Save more space in M & S block infos .
[ runtime ] Don ' t free null cache hash table in aot compiler
[ llvmonly ] Avoid disabling llvm for methods with pinvoke signatures in llvmonly mode .
[ profiler ] Replace a check with an assert in coverage_filter ().
[ sgen ] We must do bridge processing with all sgen locks taken .
Detach the current thread as soon as possible when mono_gc_pthread_exit is called .
[ runtime ] Free LLVM failure message
[ runtime ] Fix malloc backend for mempools .
Fix the build .
[ runtime ] Free temporary attr structure
[ mono ] Fix maybe - uninitialized warning .
[ jit ] Avoid emitting seq points in inlined methods .
[ aot ] Fix a memory leak .
dnsdist : Fix a sign - comparison warning in isEDNSOptionInOpt ()
dnsdist : Flush output in single command client mode
Check the packet size in dnspcap2protobuf
add a dropAction () creator for Lua
Initialize d_result to NULL on ctor
queue : win32 : fix symbol export for M_queue_exists ()
Added code to randomize values returned by rand ()
Fixed missing initialization
Added missing free ()
Fixed possible NULL pointer exception in ssl_get_ciphersuite ()
pathbar : don ' t unconditionally iterate over buttons when scrolling
window - slot : protect against no new view
list - view : don ' t crash when expanding a directory
desktop - background : stop any pending crossfades on widget destruction
toolbar : plug a couple of memory leaks
pathbar : set right return value for class method function pointer
link : plug a memory leak
window - slot : fix formatting
Ignore > 2 clicks in the icon container (# 621098 ).
mime - actions : fix double free in error path
files - view : select the pending selection
search - hit : fix mem leak
eel - canvas : ignore extra mouse button events
window : open detached tab windows at mouse position
fix memory leak in MPD Byte range objects
fix buffer overrun in gf_bin128_parse
fixed gcc warning
Fixed crash introduced by trafID checks patch
fixed # 1769 ( fuzz )
fix XML dump when file does not have tracks ( e . g . HEIF files )
Commented safety check by romain breaking SVC import
Fixed bug in image loader
Removed scanning files starting with '.'
Fixed wrong AU seq num len when hinting (# 640 )
don ' t try to dump null boxes (# 1093 )
fixed potential crash in adts reframer with broken streams - cf # 1723
fixed UNKNOWN READ
Prevent leaking memory from PIV readers
use path only if it is actually present
OpenPGP : NULL - ify free ()' d pointer
atr table reallocation fix by William Wanders
Made sure attribute CKA_TOKEN is set to TRUE .
coolkey : Make sure the object ID is unique when filling list
libopensc : initialize value returned by sc_select_file
Fixed out of bounds access in ASN . 1 Octet string
pkcs15 : log parsed TokenInfo . serial
gio : changed default symbols
fixed parsing pace output data
psql : honor ' footer ' option for expanded output
Add an explicit cast to Size to hyperloglog . c
exif_scan_JPEG_header : Added length check to prevent information leak
ext_mbstring : Fix invalid free () in php_mb_parse_encoding
Fix a fallthrough bug in TranslatorX64 :: smash
Fix a copy - paste error
tdf # 96243 don ' t crash if LibO install . wasn ' t found
XrdHttp : Add missing return statement
Recognize the -- coerce xrdcp option ( it was forgotten ).
[ XrdCl ] Treat empty redirect response as error .
[ SSI ] Unbind the request prior to teardown .
chanserv / sync : Return after kicking for RESTRICTED
anope_convert : Avoid a crash on a nickless account .
libathemecore / help : Fix memory leak when a help file cannot be opened
chanserv / antiflood : update mqueue_t . last_used when adding new messages
chanserv / moderate : don ' t moderate registrations from those who have PRIV_CHAN_ADMIN .
added more extension info .
fixed timer order .
bzero when close new_fd .
Missing delete for config reader
m_ssl_gnutls Only generate DH params when dh_params is inited
Added Buffer member
Copy the silence flags when sending update notifications .
Fixed some craq ++
Ignore this commit . * jedi mind trick *
nickflood : Allow changes to UID
Fix a bug with / SHUN deletion ( it didn ' t work )
Fixed / spynames , thanks to relax for finding the bug
Made erroneous included configs cause the reader to bail
Convert the output limit back to the parameter
m_spanningtree Ensure that TreeSocket :: Close () only runs once
browser : check window liveness before setting title
added comment to mention sk_tools_utils :: copy_to
Closing asar file should be syncronous
unzip : comment why there is no USE_INTERNAL_MINIZIP boolean option
* Fix to the gcc4 /- O0 x86 JIT compiler bug
Invalid values for saved_stamp in comput_subject_downtime_times ()
zebra : stack overrun in IPv6 RA receive code ( CVE - 2016 - 1245 )
lib , stream : fix stream sanity checks
pimd : Fix size_t zlog_err format string warning
[ bgpd ] 64 - bit bugfix in community_del_val by Jeremy Jackson < jerj @ coplanar . net >
ospfd : fix packet reception for FreeBSD 10 .
bgpd : prevent double address delete on shutdown
* src / syntax . c ( print_tree ): print something for a NULL tree
* src / augeas . c ( init_root ): check for failure of strdup
* examples / fadot . c : free the result from FA_AS_REGEXP
avoid symlink attacks in __fish_print_packages and spawning fishd
env_universal_common : always pick shmem strategy on Cygwin
fish_indent : indent " case " with the same level as " swith "
reset stop flag before starting threads
undef append_features for Classes
check ` mrb_open ` return value
Fix an off - by - one causing a buffer overflow in mirb .
Fixed dereference to null pointer in ' codegen . c ' reported by ' clang - analyzer '
fix length check in read_section_debug (); fix # 1572
fix String # inspect with MRB_UTF8_STRING
reduce invoking mrb_intern from convert_type
pluto : dpdaction = restart folloup to 24f14ba85ed250b
make sure to unpack the nonce that was calculated .
ikev2 : reject SKF payloads when the remote never negotiated them
LSW_FDMASK : Avoid signed integer overflow
ikev2 : during Child SA rekey negotiation abort when ESP proposals mismatch .
silence a coverity warning
x509 : free shallow general names after requesting crl fetch
proper fix for mtu calculation initialization
fix typo . the previous commit brome make .
Get openswan to send the remote host address to PAM during XAUTH
" Memory leak . Dynamic memory stored in ' file_content . buf ' allocated
git - svn - id : svn :// cherokee - project . com / cherokee / trunk @ 474 5dc97367 - 97f1 - 0310 - 9951 - d761b3857238
Fixes a tiny memory leak on the Digest ' nonce ' cache code .
Fixes an issue processing the X - forwarded - for header . Bug # 924 :
Fixes a memory management issue . The ' extensions ' rule match could
xkbcomp : Don ' t falsely promise from ExprResolveLhs
Bionic : Aliasing problems with frexpf
Remove a gcc - 4 . 5 compilation warning :
fix PTRDIFF_MAX check in the 32 - bit mmap wrapper
bionic : fix memory leak in get_malloc_leak_info () error path
Libraries without dt_soname are inaccessible
fix crash in tracing objtypespec
swb : fix one missing inlineSlots annotation
Fixed a memory leak in FileRef ' s OGA format detection
Use 16pix icons for size < 19
squash heap - buffer - overflow , PoC TIFF from Hardik
Added check for incorrect number of meta channels report in # 492 .
Added missing null check reported in # 817 .
Moved check for https :// github . com / ImageMagick / ImageMagick / issues / 92 .
Fixed SEGV reported in https :// github . com / ImageMagick / ImageMagick / issues / 130
Added missing return to avoid use after free .
Restored call to RelinquishMagickResource .
fix memory leak in ReadYCBCRImage as SetImageExtent failure
Added missing call to ConstrainColormapIndex .
Fixed incorrect call to DestroyImage reported in # 491 .
Fixed memory leak .
Added check for bit depth 1 .
Fixed head buffer overflow reported in : https :// github . com / ImageMagick / ImageMagick / issues / 98
Add another trace point .
fixed a warning in client ( mac )
fix a leak when unpausing evrpc requests
avoid valgrind false positive by zeroing epoll_event
Fix a reversed check in upcast_evbuffer
r15245 @ tombo : nickm | 2008 - 04 - 18 09 : 27 : 50 - 0400
http : fix leaking of response_code_line
initalize ev_res from Scott Lamb
* ftpclass . cc : don ' t send FEAT after QUIT .
* FileCopy . cc : don ' t create target file if source does not exist .
correct a signedness warning
Fixed memory leak
win : add missing break statement
Windows : set loop counters to zero in uv_loop_init
unix : don ' t run i / o callbacks after prepare callbacks
unix : NULL pipe_fname in uv__pipe_close ()
unix : fix off - by - one error in comparison
sunos : filter out non - IPv4 / IPv6 interfaces
uv - unix : fix warning : control reaches end of non - void function
unix : handle EWOULDBLOCK in uv__write ()
X3F : check for data offset limit
X - E3 to camera list
X3F / simple_decode_row : check for data offset limit
Plugin : add export to buffer_size function .
Duda : SQLite : fix return value on sql_dump ()
utils : Fix a valgrind warning about uninitialized data
server : http_parser : fix pointer protocol length
Sched : Fix a segfault when iterating timeouts
iov : buf to free non - zero allocation
mk_server : streams : fix dereference after null check ( CID 1312109 )
bin : declare missing server ctx
window - tracker : Fix memory leak
shell - doc - system : fix %- escaping code in shell_doc_system_open ()
st - box - layout : Don ' t report a paint volume when we have no allocation
shell - global : Use G_VARIANT_TYPE macro for checking
detect : add missing sigorder case
dnp3 : use BasicSearch instead of memmem
defrag : fix reconstruction
counters : don ' t run if no counters have been registered
http_heade fix , check first that we have a connp before checking transactions
flowvar : initialize new memory to prevent issues on error handling
fix uninit var usage in hhd
dns ( tcp ) - fix coverity cid 1374307
pe : imports : Actually look at FirstThunk if OriginalFirstThunk is zero .
add transition helper to format . h
Fix the build .
removed silly check for >= 0 of a supposedly unsigned value !
http2 : support > 64bit sized uploads
ssh : fix ' left ' may be used uninitialized
pop3_doing : don ' t call pop3_dophase_done () if already failed
multi : fix CURLM_STATE_TOOFAST for multi_socket
imap . c : Fixed an incorrect variable reference
mime : properly unbind mime structure in curl_mime_free ().
tool_writeout : fixed a buffer read overrun on -- write - out
fix compiler warning
don ' t use curl . haxx . se
gtls : dereferencing NULL pointer
lib / http : add missing whitespace in verbose output
http2 : Fix address sanitizer memcpy warning
http2 : abort the send_callback if not setup yet
voutf : fix bad arethmetic when outputting warnings to stderr
urlglob : avoid NULL pointer dereference
curl : do bounds check using a double comparison
curl_easy_reset : Also reset the authentication state
[ SP ] Fixed incorrect cgame syscall argument handling for Menu_Paint .
[ SP ] Win32 : Fix game incorrectly reporting low mem
[ MP ] Fix crash when drawing input fields
[ MP ] Prevent a crash in NPC_ChoosePainAnimation when NPCs are knocked down .
fix typo : s / POWOR / POWER /
get rid of strcat
ext / syck / handler . c ( syck_hdlr_add_alias ): add cast to avoid warning .
ext / zlib / zlib . c : initialize return value
comma at the end of enum is a C99ism
string . c : fix one - off bug
string . c : fix integer overflow
thread_sync . c ( condvar_ptr ): reset fork_gen after forking
m_capab : fix a possible remote crash triggered by the CAPAB parsing code .
Allow clients to have a resolved hostname of localhost
modules : wow , that was a dumb error .
blacklist : this didn ' t get committed somehow
opm : properly disable opm on blacklist deletions .
Cast to size_t before multiplication
Fixed possible NULL pointer exception in ssl_get_ciphersuite ()
Shut up gcc
diraliases : always set the tail of the list to NULL
Spit out definition of YY_CHAR early
kj :: repeat () should support subscript operator .
cmd / 8g : fix possibly uninitialized variable in foptoas .
cmd / 8g : don ' t crash if Prog -> u . branch is nil
fix bug introduced in 33293
cmd / ld : change DWARF encoding for array lengths
runtime : fix 386 , arm build
cmd / gc : rephrase the invalid indexing operation error message
runtime : don ' t stop bitmap dump at BitsDead
initialise dc variable to satisfy old compilers .
typo fix on function
Fix a memory leak in tls1_mac
X509_VERIFY_PARAM_free : Check param for NULL
Typo : should check mgf1md
return correct NID for undefined object
x509 : fix double locking problem
Fix a crash in the asn1parse command
RT4002 : check for NULL cipher in p12_crpt . c
Added error checking for OBJ_create
pkey_gost_ctrl : check for NULL after allocating pctx -> shared_ukm
set auto ecdh parameter selction for Suite B
GH1141 : Different fix , preferred by Richard .
capi_get_key : check for NULL after allocating key
Free a BIO_ADDR if DTLSv1_listen return <= 0
Missing OPENSSL_free on error path .
Updates from 1 . 0 . 0 - stable
fix potential memory leak when allocation fails
Make it possible to disable STORE .
Added a check for malloc fail .
usbutils . c uninitialised usbstat for non - primary mmqs
klondike use a memcpy
Bugfix : Free filename entry for writing config file when done with it
Put a lower limit on the nonce increment in cpu mining .
Bugfix : off - by - one in priority calculation
gif_read : Set last_name = NULL unconditionally .
policycoreutils : semodule_package : do not fail with an empty fc file
libsepol : sepol_av_to_string : clear static buffer
sestatus : free process and file contexts which are checked
libsepol / cil : Destroy classperms list when resetting classpermission
libsepol / cil : cil_reset_classperms_set () should not reset classpermission
fix small memleak in action ()
bugfix : check return value of malloc ()
support force termination in script engine
bugfix : plug a memleak in mmsnmptrapd
core bugfix : small memory leak in external command parser
fixed potential uninitialzed var access ( highly improbable )
fixed memory leak in worker instance of omelasticsearch
Fixed bug in IRC plugin when receiving data from server
core : replace constant by sizeof in a call to snprintf
irc : fix parsing of DCC filename
relay : remove unneeded check on variable irc_argv
Skip 0 - length ACL fields
Avoid a double - free when a window size of 0 is specified
Issue 719 : Fix for TALOS - CAN - 154
warc : consume data once read
Fix a crash with xmllint -- path on empty results
crypto : Enable EDDH all OpenSSL cryptolib over beta version 1 . 1 . 1 - pre8
erts : Search for heart in ports that are alive
erts : Fix beam_load assert
fix : don ' t pass colour - codes to filters
range check - set_default_value
Make a string translatable .
fix crash in limit when user enters invalid regexp
proposed fix for clearing labels
Try fixing a possible segmentation fault . I ' m not entirely sure how
imap_quote_string make room for quotes
fpgautils : add support for 57 . 6 kBd serial
Target should only be 32 bytes copied .
Bugfix : Free filename entry for writing config file when done with it
syslog requires a facility ... in more than one place
usbutils - which_intinfo not requried
klondike use a memcpy
fix multiple authentication using S / Key w / privsep
initialise salen in binresvport_sa
rbd : free owners when rbd_lock_break timeout
libtcmu : free ctx in tcmulib_initialize
fixed local DoS when UnregisterHandler was called for a not existing handler
runner : init tmp handler
ccpp : do not read data from root directories
abrt - hook - ccpp : added a TODO comment . No code changes
trivial : close inotify_fd on exec
abrt - gui : change the help accelerator to alt - H
remove some logs
Delete the TransferManager on disconnect
emacs bindings : added M - d ( kill word )
fixed multiline history
add svn_version () and global var which are
src / wav . c : Fix exif parser bug
src / aiff . c : Fix a buffer read overflow
src / wavlike . c : Prevent infinite loop on malformed file
src / aiff . c : Validate channel count .
fail on parse error
[ trunk ] Import patch from sumatrapdf team . This handle testcase 2977 . pdf . asan . 67 . 2198
reset ` reprocess_is_too_early ` when reprocessing
backlinks should be updated on rebind
rhbz : die if missing mandatory member
add missing dd_close () to save_dump_dir_from_problem_data ()
strbuf :: strtrimch () : memmove () reads bytes out of borders
bplist : Make sure to detect integer overflow when handling unicode node size
xplist : Prevent heap buffer overflow when parsing empty tags
Plugging memory leak in write_unicode
fix crash on unknown shape - reported by Ryan Schmidt
quickfix for metapost
fix minor textbuffer leak , Bug 288 by Toby
scrolling in empty windows crashed
Fixed glib warning with autologging and queries .
Fixed potential misinterpretation of boolean logic
define IN6_ARE_ADDR_EQUAL macro for WIN32
Fixing ecma_create_arguments_object : adding deref for allocated ecma - string
Adding assertion that interpreter doesn ' t receive non - empty normal ecma completion value .
print - dhcp6 . c : add TCHECK (* dh6o ) in dhcp6opt_print ()
PPP : check PAP AREQ length too , see commit 32118c6
needed for previous checkin
check if connection was accepted before logging
Remove too early set of auth_user
Check if auth_user is set .
dnslookup : missing active count inc
Check if auth_user is set .
fix a warning i just introduced
added missing g_free ()
Free the decision
cbson : PySequence_GetItem returns a new ref
[ Bugfix ] Render " Ok " and " Cancel " button correctly when compiled with GNUTLS .
fix leak in handle_propfind ()
gcc complains about _MSC_VER being an undefined macro .
Fix an allocation problem in inf_text_fixline_buffer_buffer_insert_text ()
destroy image and set MemoryError on overflow check failure
idevice : free ssl object when SSL handshake fails
make sure ' length ' is >= 0 in afc_file_read
fixes depth check in format_string ( can be 0 )
Fixed an array overflow problem in the JPC decoder .
don ' t echo query params on invalid requests to redirect URI ; closes # 212
Add some messages to PAM close session
BUG : Remove memory leak in np . place
BUG : erroneous use of sprintf
BUG : porting bugfix from refactor @ 3da77df
tcti_ldr : load a specific so version
tpm2_getmanufec : only attempt to persist the EK if a handle was provided
Check the return value of fscanf () when reading a PID file .
fix : prefix 0x67 missed .
x86 : instruction length must be <= 15
[ M68K ] handle -> printer_info need to be check
systemz : fix # 1062
mips : sanity check for input code length of Mips64
fix wrong function name in merge of GPR # 596
Fixing ' in ' opcode handler .
cgfs : fix dorealloc ' s batch allocation
pass the UDI up if existant
fix accidently reverted code
rewind wpa file descriptor when uploading
osdep : Fix possible infinite loop ( Closes : # 1582 ).
dracut - install : handle more ldd errors
rgw : fix subuser key name when purging subuser keys
msg : MDentryLink has a private destructor
libcephfs : destroy cmount on ceph_shutdown ()
mon : fix bad return test !
mds : drop auth pins before waiting for dir unfreeze
mds : fix uninit field in CInode
mon : Elector : clean ack set on election start
mon : Monitor : get required features from monmap
include / ceph_hash . cc : reduce scope of a var in ceph_str_hash_rjenkins ()
librbd : properly handle replay of snap remove RPC message
librados : take lock when signaling notify cond
osd : clear_prior when activating
msg : async : trim already sent for outcoming_bl more efficiently
monmap : clear addr_name map on calculating ranks
filestore : verify that op_seq is nonzero
PG : don ' t compare auth with itself
fragtree_t : fix get_leaves_under ()
filejournal : use :: encode () wrapper func for Transaction
mds : Keep journaler in readonly mode until replay completes .
mds : properly free CInode :: ValidationContinuation
librbd : add create_async_context_callback helper
rados : statically allocate time
lockep : fix leak in failure / assert path
osd : clear INCONSISTENT if scrub detects no errors
kclient : check for ENAMETOOLONG in lookup
mds : choose reasonable state after xlock finishes
PG : clear peering flag when we transition from Peering to Reset
crypto : don ' t hash zero sized buffer
osd : Reset () the snaptrimmer on shutdown
mon : send mgrdigest promptly to new active mgr
cls_rgw : fix rgw_bucket_init_index
rgw : Error check on return of read_line ()
cls / rbd : fix note
PG : set role for replicated even if role != shard
rbd : disk usage on empty pool no longer returns an error message
set bl length in buffer :: read_file
librbd : force - remove journal when disabling feature and removing image
rgw : fix printf format warning
librados : fix unsigned vs . signed warning
osd : gracefully exit when create_object_store () returns NULL
osd : avoid polluting peer_info with acting [ 0 ]
mon : crush straw_calc_version value is 0 or 1 not 0 to 2 .
Event : process event before pop it to avoid releasing
add stray_prior_path to store path before rename
PG : strays don ' t peer ( only the primary does )
doc : Add usage comment to librbd :: RBD :: AioCompletion
mds : put pending ( not added ) caps in reply caps
msg : initialize footer
rgw : fix a typo
osd : fix uninit value in scrub message
osd : Update object state after removing watch from object info
rgw : fix flush_read_list () error msg
msg : MLogAck has a private destructor
OSD : lock recovery_wq before debug output on finish_recovery_op
rbd - mirror : gracefully fail if object map is unavailable
mds : update current_parent_since for newly snapshotted directory
rgw : fix use - after - free
rgw : move coroutines out of anonymous namespace
rgw : was missing return value
mds : look at active ( not projected ) inode when issuing truncation
osd_types : add missing osd op flags
osd : init my_stat
osd : drop obsolete ' filestore ' bool option
mds : set bad dirfrag ' s version to 1
msg : MMDSMap has a private destructor
rbd : allow users to remove rbd image when Ceph cluster becomes full .
mds : fix can_rdlock_soon vs xlock on FileLock
mgr : fix OSDMap notifications
filestore : free fiemap used for testing fs capabilities
crush / CrushWrapper : fix out of bounds access
auth : fix NULL pointer access when trying to delete CryptoAESKeyHandler instance
kclient : setxattr for virtual xattr is not allowed
add ceph_fsid_set helper
add rename check_access
osd : fix trailing + in pg state string rendering
osdc : fix pgls_filter op encoding
mds : add IFLOCK to wait bits to prevent collisions with lock branch
kclient : reset session cap seq # on reconnect
osd : fix build_initial_pg_history
osd : fix leaked OSDMap
Rgw : check the return value when call fe -> run ()
mon : Monitor : drop useless ' todo ' comment
mds : zero out the layout in handle_client_setlayout
librados : fix use without NULL check in rados_pool_list
msg : MClientLease has a private destructor
kclient : fix NULL dereferencing oops
Changed name extension of calendar files from " vcf " to " ics ".
bug - fix for bz # 1352432
Fixed malloc . h path on iOS target platform
fix bit - length check in der_decode_raw_bit_string ()
fix FOLDS_FOLD_ADDR_BUK () for remove warning .
jpeg : Fix another possible buffer overrun
Added vim modeline
EbmlElement : don ' t read beyond end of buffer when reading variable length integers
fix memory leak
tar : fix crash on broken tar file .
Add a bounds check to ByteArray :: Get ().
fixed firejail symlink problem for -- private - bin option
added missing pclose to stop zombies from forming
gitlab # 179 - rsvg_filter_primitive_free (): Check the GStrings for NULL before freeing them
bgo # 748608 - Destroy cairo_t after use
dosfslabel : Do not read beyond string length ( Closes : # 709587 ).
set_fat (): Fix off - by - 2 error leading to corruption in FAT12
fix type mismatch anywhere int != int32 ( Vorne )
Be more careful when calculating sinc () for small arguments
Reduced the max memory use and image size in 64 - bit builds
Fixed a GIF decoding bug ( divide by zero )
ares : Prevent buffer overlow in ares_parse_a_reply ( CVE - 2017 - 9454 )
buildFHSUserEnv : change to root directory after chroot
made suidrt . c work
util : make sure to close the authfile before returning success
Fixed memory leak when an invalid timezone was found
Flush the bus before exit
remove invalid free
regcomp : use long jumps if there is any possibility of overflow
regcomp . c : Prevent integer overflow from nested regex quantifiers .
fixed memory leak
squash heap - buffer - overflow , PoC TIFF from Hardik
libvncserver : scale : cast to 64 bit before shifting
LibVNCClient : remove now - useless cast
rfbserver : don ' t leak stack memory to the remote
libvncclient : bail out if unix socket name would overflow
fix a used - before - set error in im_vips2dz
Bug 700947 : Add missing ENDTRY opcode in try / catch / finally byte code .
virDomainGetTime : Deny on RO connections
qemu : Add missing lock in qemuProcessHandleMonitorEOF
ofproto : Fix OVS crash when reverting old flows in bundle commit
Always check the number of coefficients
Throw on skipping an invalid type .
Fixed possible credentials leaking reported by Alex Birsan .
Changed to using carbon module delegate methods
Log an error when SPIFFS_mount fails
typo in shader
stb_leakcheck : Make stb_leakcheck_free actually free .
branches / zip : backport r3848 from 6 . 0 branch
continued fix for reset master
fix Bug # 15907954 - SETTING INNODB_MIRRORED_LOG_GROUPS = 2 LEADS TO SERVER
Added missing DBUG_ENTER ( bug in last push )
Acquire the trx_sys_t :: lock before traversing the trx list .
reverting a patch
Fixed a bug in vis . c
fixed compiler error
Addendum to the fix for bug 43029
correct bug in code
Edited fix for # 1224 .
pb2 fix : one machine on pb2 didn ' t like TABLE :: map
blockSend and unblockSend are not declared since
correct parameter of hash_search
branches / zip : ha_innobase :: add_index (): Check for ! innodb_table .
fix github issue # 127
add NULL ctx argument check to wolfSSL_X509_STORE_CTX_set_time ()
fix aes ctr cast
Fixed issue with ECDH - ECDSA cipher suites rejecting certificates without
discard session chain in SSL_clear
initialize extra name entries to 0
Fix the WOLFSSL_SEP (-- enable - sep ) build scenario where extCertPoliciesNb is not available .
[ PATCH ] uml : activate_fd : return ENOMEM only when appropriate
Btrfs : add tree block level sanity check
drm / mgag200 : Constify function pointer structs
[ PATCH ] tpm : fix memory leak
drm / fb - helper : Do the ' max_conn_count ' zero check
staging : comedi : adv_pci_dio : do board reset early in (* auto_attach )
xen / xenbus : fix overflow check in xenbus_file_write ()
ath6kl : Release the memory allocated for the firmware
xilinx_emaclite : Call dev_consume_skb_any instead of dev_kfree_skb .
i2c : xiic : must always write 16 - bit words to TX_FIFO
dlm : sctp_accept_from_sock () can be static
intel - iommu : double kfree ()
usb : gadget : f_subset : fix missing unlock on error in geth_alloc ()
signal : always clear sa_restorer on execve
fs / binfmt_misc . c : add terminating newline to / proc / sys / fs / binfmt_misc / status
drivers / net / skfp : if ! capable ( CAP_NET_ADMIN ): inverted logic
sched / deadline : Skip in switched_to_dl () if task is current
Input : ati_remote - add missing input_sync ()
[ SCSI ] fcoe : fix missing error check in call to fcoe_if_init
[ SCSI ] mpt fusion : make mptsas_smp_handler update resid
wlcore : fix unsafe dereference of the wlvif
mm : highmem : export kmap_to_page for modules
[ PATCH ] auerswald . c : % zd for size_t
efikasb / mx : fix usbh1 initialisation
staging : rtl8192e : don ' t just print an error and continue
arm64 : Align CMA sizes to PAGE_SIZE
omap : 4430SDP : Register the card detect GPIO properly
[ IPV4 ]: ipv4_is_lbcast () misannotations
drm / radeon / kms / evergreen : No EnableYUV table
KVM : MMU : fix shrinking page from the empty mmu
quota : info leak in quota_getquota ()
gpio : palmas : fix a possible NULL dereference
[ PATCH ] it87 : Fix oops on removal
drm : work around EDIDs with bad htotal / vtotal values
s390 / con3270 : testing return kzalloc retval
iwlwifi : fix kernel oops when ucode DMA memory allocation failure
bridge : cleanup : remove unneed check
ext4 : let ext4_group_add_blocks () handle 0 blocks quickly
regmap : Don ' t attempt block writes when syncing cache on single_rw devices
af_rxrpc : Handle frames delivered from another VM
isicom : Deinline drop_dtr , save 112 bytes
iio staging : fix lis3l02dq , read error handling
drm : fixup improper cast .
staging : media : lirc : Use pr_ * instead of printk
drm / i915 : Only clear the GPU domains upon a successful finish
staging : dwc2 : when dma is disabled , clear hcd -> self . uses_dma
[ IB ] mthca : fix off by one in clr_int calculation
[ SCSI ] zfcp : prevent fc_remote_port_delete calls for unregistered rport
i2c / ds1374 : Check workqueue creation status
mmc : dw_mmc : Don ' t print data errors
ARM : OMAP : Initialize PWM backlight enable_gpio field
rtc : rtc - twl : Switch to using threaded irq
pinctrl : fix a minor harmless typo
i4l : leak in eicon / idifunc . c
staging : android : logger : enforce GID and CAP check on log flush
[ SCSI ] qla2xxx : Don ' t hardcode fw_transfer_size for ISP2 [ 45 ] xx parts .
dma : of - dma . c : fix memory leakage
bnx2 : Zero out context memory for 5709 .
mm : node_states : introduce N_MEMORY
[ ALSA ] Fix possible deadlocks in sequencer at removal of ports
mm : fix NULL ptr deref when walking hugepages
kexec / uefi : copy secure_boot flag in boot params across kexec reboot
ext3 : update ctime when changing the file ' s permission by setfacl
iwlwifi : mvm : BT Coex - fix shared antenna check with new API
libceph : Remove spurious kunmap () of the zero page
ext4 : online defrag is not supported for journaled files
cfg80211 : reset auth algorithm
[ IA64 ] Correct pernodesize calculation .
[ ETH ]: Validate address in eth_mac_addr
s390 / hugepages : clear page table for sw large page emulation
iwlwifi : mvm : allow RSSI compensation
cxlflash : off by one bug in cxlflash_show_port_status ()
ext4 : Make sure the MOVE_EXT ioctl can ' t overwrite append - only files
igb : limit EEPROM access
ASoC : wm9713 : fix regmap free path
intel - iommu : Fix reattaching of devices to identity mapping domain
xfrm : Force SA to be lookup again if SA in acquire state
[ PATCH ] IB : Initialize qp -> wait
KVM : x86 emulator : fix lea to really get the effective address
posix - timer : Don ' t call idr_find () with out - of - range ID
tracepoints : fix disable
gpio - 74x164 : Fix buffer allocation size
usb : Do not allow usb_alloc_streams on unconfigured devices
[ ARM ] 5072 / 1 : corgi_pm : don ' t register devices on non - corgi machines
cfg80211 : Process all pending regulatory requests / hints
[ PATCH ] pcmcia : socket . functions starts with 1
rt2x00 : Add RT539b chipset support
x86 : Fix panic when handling " mem ={ invalid }" param
mtip32xx : remove unneeded variable in mtip_cmd_timeout ()
[ media ] davinci : vpbe : fix build error when CONFIG_VIDEO_ADV_DEBUG is enabled
qeth : Clear SBALF15 in any case for output buffers .
[ PATCH ] bug in futex unqueue_me
x86 / hpet : Check for irq == 0 when allocating hpet MSI interrupts
ceph : exclude snapdir from readdir results
USB : wusb : check CHID is all zeros before stopping the host
xfs_file_last_byte () needs to acquire ilock
staging : vt6655 : check ieee80211_bss_conf bssid not NULL
ath6kl : fix memory leak in ath6kl_fwlog_block_read ()
[ NETFILTER ]: bridge : fix double POSTROUTING hook invocation
arch / powerpc / sysdev / ehv_pic . c : add missing kfree
mm : Adjust do_pages_stat () so gcc can see copy_from_user () is safe
sch_gred : should not use GFP_KERNEL while holding a spinlock
drm : Fix a bug in the range manager .
fuse : invalidate target of rename
bridge : netfilter : orphan skb before invoking ip netfilter hooks
staging : comedi : fix typo in error message
drm / radeon / vm : don ' t attempt to update ptes if ib allocation fails
[ PATCH ] rcu : Avoid kthread_stop on invalid pointer if rcutorture reader startup fails
mfd : Handle missing WM8350 platform data
alpha : long constant
drm / i915 : Fix intel_tile_width () parameters
sparc64 : Force the execute bit in OpenFirmware ' s translation entries .
ASoC : Intel : fix missing mutex
[ SCSI ] bfa : off by one in bfa_ioc_mbox_isr ()
staging : vt6655 : Use NULL instead of 0
drivers / rtc / rtc - pcf8563 . c : fix pcf8563_irq () error return value
staging : vt6655 : dpc . c : replace memcpy () by ether_addr_copy () using coccinelle
davinci_emac : off by one
drivers / net / phy / mdio_bus . c : call put_device on device_register () failure
[ XFRM ]: xfrm_address_t annotations
KVM : MCE : Add MCG_SER_P into KVM_MCE_CAP_SUPPORTED
Staging : android : timed_gpio : Properly discard invalid timeout values .
rtl8180 : avoid potential NULL deref in rtl8180_beacon_work
vhost / scsi : Check LUN structure byte 0 is set to 1 , per spec
perf timechart : Fix SIBGUS error on sparc64
smsc - ircc2 : Fix section mismatch derived from smsc_ircc_pnp_probe ()
[ SCSI ] qla2xxx : Don ' t register to legacy interrupt for ISP82xx .
drm / exynos : dsi : modify a error type when getting a node failed
x86 / mce : Fix CPU hotplug and suspend regression related to MCE
[ PATCH ] Missed error checking for intent ' s filp in open_namei ().
iwlwifi : mvm : Always use SCAN_TYPE_FORCED
mmc : core : Fix use of uninitialized data in mmc_send_if_cond .
staging / sep : Fix smatch false positive about potential NULL dereference in sep_main . c
GFS2 : fsck . gfs2 reported statfs error after gfs2_grow
HID : hid - picolcd : Fix memory leak in picolcd_debug_out_report ()
uglify while_each_pid_task () to make sure we don ' t count the execing pricess twice
drm / i915 : Fix SRC_COPY width on 830 / 845g
lockdep : fix debug_show_all_locks ()
hwmon : ( hih6130 ) Fix missing hih6130 -> write_length setting
[ media ] tvp514x : Fix double free
sparc32 : Need to close openned RTC device just like sparc64 .
[ media ] si2157 : advertise Si2158 A20 firmware
tg3 : Disable 5717 serdes and B0 support
md : fix bug in handling of new_data_offset
[ PATCH ] struct path : convert openpromfs
hwmon : ( w83793 ) Fix vrm write operation
mtd : nand : update mtd_to_nand ()
dmaengine : bcm2835 - dma : Fix memory leak when stopping a running transfer
brcmsmac : handle non pci in ai_deviceremoved ()
ecryptfs : avoid to access NULL pointer when write metadata in xattr
tuntap : set SOCK_ZEROCOPY flag during open
spi_bfin : use more useful GPIO labels
phy layer : fix genphy_setup_forced ( don ' t reset )
staging : csr : remove unneeded call to memset ().
bna : Check for NULL before deref in bnad_cb_tx_cleanup
pch_phub : Fix MAC address writing issue for LAPIS ML7831
[ POWERPC ] arch / powerpc / sysdev / qe_lib of_node_get cleanup
[ PATCH ] Mobil Pentium 4 HT and the NMI
[ BNX2 ]: Update version to 1 . 6 . 8 .
xfs : Fix quota type in quota structures when reusing quota file
amdkfd : use atomic allocations within srcu callbacks
stmmac : fix check for phydev being open
KVM : i8259 : simplify pic_irq_request () calling sequence
btrfs : fix dip leak
modedb : fix incorrect sync and vmode flags for CVT modes
drm : copy the right data back to userspace for getreserved contexts ioctl
ASoC : Fix off - by - one bug in WM8962 register cache size configuration
i2c : mv64xxx : continue probe when clock - frequency is missing
gpio : em : Setup gpiochip -> dev
[ PATCH ] ARM SMP : Mark mroe CPU init data with __cpuinitdata
[ PATCH ] myri10ge : update driver version to 1 . 1 . 0
ioat : fix type mismatch for -> dmacount
[ SCSI ] ibmvscsi : Correct DMA mapping leak
alpha : Fix uninitialized value in read_persistent_clock .
leds : Fix wrong loop direction on removal in leds - ams - delta
fb_defio : Push file_update_time () into fb_deferred_io_mkwrite ()
HID : multitouch : force retrieving of Win8 signature blob
cifs : Read buffer overflow
[ IA64 ] Remove sparse warning from unwind code
[ SCSI ] be2iscsi : memset wrb for ring create
perf script : Fix a NULL pointer dereference
bridge : mcast snooping , fix length check of snooped MLDv1 / 2
sky2 : Call dev_kfree_skb_any instead of dev_kfree_skb .
[ PATCH ] sys_ioprio_set : minor do_each_thread + break fix
iio : iio : Fix iio_channel_read return if channel havn ' t info
ath9k_hw : index out of bounds
mac80211 : send control port protocol frames to the VO queue
staging : lustre : Remove unnecessary parentheses .
KVM : x86 : apply kvmclock offset to guest wall clock time
[ BNX2 ]: check return of dev_alloc_skb in bnx2_test_loopback
usb : gadget : storage : release superspeed descriptors .
tracing : Have trace buffer point back to trace_array
[ ALSA ] soc - Report errors from snd_soc_dapm_set_endpoint ()
ALSA : hda - Remove superfluous pcm NULL check
tools / firewire : nosy - dump : work around segfault in decode_fcp
drivers / net / eql . c : prevent reading uninitialized stack memory
[ MIPS ] Sibyte : Fix name of clocksource .
cifs : ensure that vol -> username is not NULL before running strlen on it
fsnotify : kzalloc fsnotify groups
KVM : VMX : Fix pending NMI - vs .- IRQ race for user space irqchip
tipc : fix out of bounds indexing
mac80211 : fix potentially redundant skb data copying
HID : egalax : Use kzalloc
mmc : sdhci - esdhc - imx : change default watermark level and burst length
drm / i915 : Drop any active reference before unbinding
[ S390 ] dasd : Use default recovery for SNSS requests
bpf : fix BPF_MAP_LOOKUP_ELEM command return code
mv643xx_eth : prevent interrupt storm on ifconfig down
USB : atmel_usba_udc : fix freeing irq in usba_udc_remove ()
ath9k : make sure main_rssi is positive
irda : Fix irda_getname () leak
svcrdma : RDMA support not yet compatible with RPC6
memcg : fix leak of accounting at failure path of hugepage collapsing
[ PATCH ] janitor : reiserfs : super . c - vfree () checking cleanups
[ MIPS ] add io_map_base to pci_controller on Cobalt
drivers / net / cpmac . c : Correct code taking the size of a pointer
usb : isp1760 : check for null return from kzalloc
tcp : tcp_disconnect () should clear window_clamp
net : make ipv6 bind honour freebind
[ PATCH ] enable booting a NUMA system where some nodes have no memory
[ media ] v4l : vsp1 : Fix VI6_DISP_IRQ_ENB_LNEE macro
taskstats : cgroupstats_user_cmd () may leak on error
tty / serial : at91 : correct buffer size used in DMA
fs : Fix theoretical division by 0 in super_cache_scan ().
[ SPARC64 ]: Fix pfn_pte () build failure .
usb : gadget : uvc : memory leak in uvcg_frame_make ()
iio : kfifo_buf improve error handling in read_first_n .
drm / nouveau : Do not leak client objects
i386 : arch / x86 / math - emu / fpu_entry . c warning fix
sfc : Clear I2C adapter structure in falcon_remove_nic ()
[ ATA ] Add named constant for ATAPI command DEVICE RESET
dlm : avoid unnecessary search in search_rsb
ALSA : snd - usb : Fix uninitialized variable usage
[ SCSI ] ipr : Bump version
[ MIPS ] Fix return type of gt64120_irq .
clk : tegra : Use definition for pll_u override bit
[ S390 ] fix initialization of stp
ALSA : hda - Don ' t actually write registers for caps overwrites
iwlwifi : unlock on error path
KVM : Fix coalesced interrupt reporting in IOAPIC
bonding : always update last_arp_rx on packet recieve
perf : Fix inherited events vs . tracepoint filters
Staging : wlan - ng : fix style in p80211netdev . c
[ PATCH ] uml : const a variable
x86 : Use string instruction memcpy on AMD Fam11h
x86 : fix printout ugliness in cpu info printk
char / tpm : Fix unitialized usage of data buffer
x86 , vm86 : clean up invalid_vm86_irq ()
staging : comedi : cb_pcidas : check for failure of subdev_8255_init
stmmac : fix max coal timer parameter
viafb : reduce I2C timeout and delay
cxl : Plug irq_bitmap getting leaked in cxl_context
[ MIPS ] Fix oprofile module unloading
pasemi_mac : Access iph -> tot_len with correct endianness
x86 , efi : Check table header length in efi_bgrt_init ()
drm / i915 : Wrap - EIO send - vblank event for failed pageflip in spinlock
omap3 : Fix booting if package is uninitialized
[ PATCH ] frv : fix uninitialised variable in serverworks driver
Input : ab8500 - ponkey - fix IRQ freeing in error path
ixgbe : reinit_locked () should be called with rtnl_lock
ipv6 : udp : fix the wrong headroom check
sdhci : don ' t warn about sdhci 2 . 0 controllers
firewire : fix ioctl () return code
misc : genwqe : fix return value check in genwqe_device_create ()
ath9k : Use memcpy in ath_clone_txbuf ()
ARM : mach - imx : mach - imx6q : Fix sparse warnings
UIO : uio_pdrv : fix memory leak
[ SCSI ] mptfusion : Set fw_events_off to 1 at driver load time .
i810fb : the pseudo_palette is only 16 elements long
mac80211 : pass AP vif pointer for VLANs
[ PATCH ] i386 : Terminate backtrace fallback early if unwinder stack pointer is zero
ARM : dma - api : fix off - by - one error in __dma_supported ()
[ ALSA ] system timer : clear correction value when timer stops
[ PATCH ] USB : fix gadget_is_musbhdrc ()
e1000e : potentially incorrect return for e1000e_setup_fiber_serdes_link
adm8211 : fix cfg80211 band API conversion
PCI : fix bridge I / O window bus - to - resource conversion
thinkpad - acpi : restrict procfs count value to sane upper limit
ipv6 : Export nf_ct_frag6_gather ()
drm / i915 : fix lane bandwidth capping for DP 1 . 2 sinks
ath5k : add missing include for debug code
arm64 / BUG : Show explicit backtrace for WARNs
[ PATCH ] m68k : isa_ { type , sex } should be exported
um : finally kill -> init_str leaks
Staging : speakup : free after input_unregister_device ()
[ PATCH ] x86_64 : free_bootmem_node needs __pa in allocate_aperture
ima : Do not free ' entry ' before it is initialized
perf_events : Fix invalid pointer when pid is invalid
Btrfs : Fix balance panic
ALSA : Fix a Oops bug in omap soc driver .
net : make ipv6 PKTINFO honour freebind
[ SCSI ] iscsi : redirect fix
HID : core : fix reporting of raw events
crypto : omap - sham - fix concurrent sha1 calculations
batman - adv : Only put gw_node list reference when removed
perf session : Skip event correctly for unknown id / machine
scsi : don ' t add scsi_device if its already visible
ring - buffer : Fix first commit on sub - buffer having non - zero delta
[ PATCH ] ip_output : account for fraggap when checking to add trailer_len
mac80211 : initialize power mode for mesh STAs
NFSD : Put exports after nfsd4_layout_verify fail
[ PATCH ] KVM : Avoid oom on cr3 switch
libceph : init event -> node in ceph_osdc_create_event ()
mwifiex : corrections in PCIe event skb handling
ARM : mx51 : Fix the address space length for SSI
rtnetlink : Fix VF IFLA policy
wan : wanxl : add pci_disable_device in case of error
drm / msm : fix memory leak
igb : reset PHY after recovering from PHY power down
net : make all team port device link events urgent
md : avoid possible spinning md thread at shutdown .
usb - storage : Add 090c : 1000 to unusal - devs
staging : ozwpan : prevent overflow in oz_cdev_write ()
mfd : wm8994 : Silence bogus warning in wm8994_device_init ()
ath9k_hw : add default chainmask for AR9462
btrfs : don ' t BUG_ON allocation errors in btrfs_drop_snapshot
iucv : fix memory leak in cpu hotplug error path .
scm : Capture the full credentials of the scm sender
/ drivers / staging / dgrp : Fixed warning unchecked sscanf return value
drm / nouveau : fix crash with noaccel = 1
ASoC : generic - dmaengine - pcm : use a more common dma name
KVM : ensure that memslot userspace addresses are page - aligned
ALSA : sb16 - info leak in snd_sb_csp_ioctl ()
Drivers : hv : Fix a memory leak
iwlwifi : fix rx signal quality reporting in dmesg
[ media ] gspca - ov519 : Fix a regression for ovfx2 webcams
drm / nouveau / core : drop the pointer value in debug printk output
drivers / net / usb / usbnet . c : Use FIELD_SIZEOF macro in usbnet_init () function .
sh : intc : Allocate subgroup virq backing desc directly .
cifs : fix off - by - one bug in build_unc_path_to_root
atmel_usba_udc : Add missing kfree () in usba_udc_remove ()
sunrpc : copy scope ID in __rpc_copy_addr6
drm / ttm : Clear the ghost cpu_writers flag on ttm_buffer_object_transfer .
tg3 : remove open - coded skb_cow_head .
dm snapshot : fix a possible invalid memory access on unload
[ ALSA ] soc - 0 . 13 ASoC DAPM bug fix for unnamed streams
ext3 : explicitly remove inode from orphan list after failed direct io
mmc : card . h : Use NULL instead of 0 for END_FIXUP
[ media ] s5p - fimc : Fix return value of __fimc_md_create_flite_source_links ()
GFS2 : write_end error path fails to unlock transaction lock
arm64 : drop unnecessary cache + tlb maintenance
drm : missing drm_vm . c changes for consistent maps
[ XFS ] Wait for all I / O on truncate to zero file size
bnx2x : fix DMAE timeout according to hw specifications
[ MIPS ] Sibyte : Fix interrupt timer off by one bug .
usb : message : Fixed parenthesis error in sizeof function .
KVM : VMX : Stop invalid guest state emulation on pending event
[ IA64 ] mbcs_init () should give up unless running on sn2
xen / xenbus : export xenbus_dev_changed
Staging : Fix leak in drivers / staging / at76_usb . c
drm / i915 : switch to default context on idle
fuse : fix d_revalidate oopsen on NFS exports
Btrfs : Don ' t case unsigned long to int in bio submission
kconfig : initialize the screen before using curses ( 3 ) functions
ext4 : Fix update of mtime and ctime on rename
gpio / ep93xx : Use irq_set_handler_locked ()
sctp : fix possible seqlock seadlock in sctp_packet_transmit ()
sh64 : re - add the __strnlen_user () prototype
drm / i915 : Don ' t kick - off hangcheck after a DRI interrupt
drm / exynos : exynos_hdmi : Pass correct pointer to free_irq ()
rt2x00 : fix a wrong parameter for __test_and_clear_bit () in rt2x00rfkill_free ().
mfd : Check for twl4030 - madc NULL pointer
clk : make sure parent is not NULL in clk_fetch_parent_index ()
ASoC : core : Fix dai_link dereference .
Phonet : fix double free in GPRS outbound packet error path
rocker : print switch ID consistent with phys_switch_id sysfs node
thermal : return an error on failure to register thermal class
sched : Disallow sched_attr :: sched_policy < 0
perf annotate browser : Fix behaviour of Shift - Tab with nothing focussed
cpufreq : e_powersaver : call CPUFREQ_POSTCHANGE notfier in error cases
ASoC : wm8731 : Check for clk_prepare_enable () error
perf ui hist browser : Fix segfault on ' a ' for annotate
[ ALSA ] Fix substream to check in PCM drain
[ PATCH ] key : plug request_key_auth memleak
[ S390 ] cio : fix memleak in resume path
drivers / char / hpet . c : fix information leak to userland
cciss : handle allocation failure
ext4 : check free block counters in ext4_mb_find_by_goal
gpio : adp5588 : remove unnecessary OOM messages
drm : Fix DRM_FORCE_ON_DIGITAL use
usb : phy : ab8500 - usb : fix phy tuning value select logic
btrfs : quota : Update quota tree after qgroup relationship change .
isdn : avoid copying too long drvid
ath10k : free wmi mgmt event skb when parsing fails
drm : drm_ioctl () should zero - init extra data
ALSA : hda / realtek - Create " Bass Speaker " for two speaker pins
perf session : Don ' t call dump_sample () when evsel is NULL
drivers / rtc / rtc - da9052 . c : register ability of alarm to wake device from suspend
pwm : omap - dmtimer : Potential NULL dereference on error
ALSA : hda - Force to reset IEC958 status bits for AD codecs
ASoC : fsi : fixup pm_runtime_disable () timing on fsi_probe ()
mfd : t7l66 MMC platform data
dm : raid fix write_mostly arg validation
ARM : at91 : remove empty at91_init_serial function
arch / powerpc / sysdev / qe_lib / qe . c : Add of_node_put to avoid memory leak
r6040 : use ETH_ZLEN instead of MISR for SKB length checking
usb : gadget : fusb300_udc : fix error return code in fusb300_probe ()
drm / radeon / kms : fix typo in spread spectrum code
KVM : Fix buffer overflow in kvm_set_irq ()
ARM : PL08x : prevent dma_set_runtime_config () reconfiguring memcpy channels
Btrfs : btrfs_add_ordered_operation : Fix last modified transaction comparison .
[ SCSI ] gdth : fix buffer overflow
PCI : eliminate double kfree in intel - iommu initialization
[ PATCH ] libata : kill illegal kfree ( id )
mtd : mxc_nand : set owner field to prevent module unloading when in use
ASoC : arizona : Store the DAI clock ID when setting
gpio : delete unneeded test before of_node_put
tracing / ftrace : make nop tracer reset previous entries
ARM : Fix warning : unused variable ' highmem '
[ PATCH ] readahead : reset cache_hit earlier
cris : fix potential NULL - pointer dereference
mmc : card : Avoid null pointer dereference
dmaengine : ste_dma40 : fix error return code
ARM : OMAP2 +: only WARN if a GPMC child probe function fail
ASoC : wm8995 : Return - EINVAL if device ID mismatch
rcar - hpbdma : add max transfer size
rt2800usb : initialize H2M_INT_SRC register
[ SCTP ]: Validate the parameter length in HB - ACK chunk .
usb : m66592 - udc : Fixed bufnum of Bulk
phy : fix phy address bug
USB : serial : fix up bug with missing {}
i40evf : Add more info to interrupt vector names
mac80211 : add status_driver_data array to ieee80211_tx_info
USB : whiteheat : fix memory leak in error path
ath9k : fix massive rx packet loss issue
wl12xx : update PLT initialization for new firmware
drivers / net / ax88796 . c : Return error code in failure
ext4 : error proc entry creation when the fs / ext4 is not correctly created
iwlwifi : mvm : initialize the cur_ucode upon boot
[ PATCH ] USB : check for device in zd1201_resume
staging : wilc1000 : off by one in wilc_wfi_cfg80211_mgmt_types
firewire : cdev : fix another memory leak in an error path
pinctrl : lpc18xx : fix schmitt trigger setup
drm_vblank_get : don ' t WARN_ON in case vblanks are not initialized
usb : gadget : f_printer : actually limit the number of instances
ath9k : Clear HT info on starting the driver
[ media ] media : davinci_vpfe : clear the output_specs
staging : rts_pstor : Complete scanning_done variable
GFS2 : quotas not being refreshed in gfs2_adjust_quota
pata_octeon_cf : fix broken build
gpio / samsung : Complain loudly if we don ' t know the SoC
af_unix : implement socket filter
wl18xx : create per - chip - family private storage
vmw_pvscsi : fixup tagging
drm / amdgpu : fix userptr flags check
leds : pca963x : Add missing initialiation of struct led_info . flags
f2fs : persist system . advise into on - disk inode
[ PATCH ] fs / compat . c : fix ' if ( a |= b )' typo
usb : musb : blackfin : call gpio_free () on error path in musb_platform_init ()
Btrfs : fix memory leak in start_transaction ()
ext4 : use stream - alloc when mb_group_prealloc set to zero
drivers / char / applicom . c : fix information leak to userland
[ media ] media : rcar_vin : Reject videobufs that are too small for current format
md : add takeover support for raid4 -> raid5 conversion .
hsi : nokia - modem : fix uninitialized device pointer
drm : crtc : integer overflow in drm_property_create_blob ()
drm / nouveau / fifo / nvc0 : bash some magic reg to make uevent interrupt work
ext4 : add missing space to ext4_msg output in ext4_fill_super ()
Btrfs : remove dead code
ata : fix sparse warning in libata . h
Btrfs : fix crash when starting transaction
ALSA : hda / realtek - Add more entry for enable HP mute led
[ IA64 - SGI ] Do not request DMA memory for BTE
ALSA : hda - check NULL pointer when creating SPDIF controls
wl12xx : set max_sched_scan_ie_len correctly
alloc_percpu : fix UP build
[ media ] ir - lirc - codec . c : don ' t leak lirc -> drv - rbuf
arm / arm64 : KVM : fix missing unlock on error in kvm_vgic_create ()
[ media ] solo6x10 : Fix solo_eeprom_read retval type
ALSA : asihpi - Fix potential Oops in snd_asihpi_cmode_info ()
ARM : footbridge : fix overlapping PCI mappings
mwifiex : fix Coverity SCAN CID 709078 : Resource leak ( RESOURCE_LEAK )
simple_xattr : permit 0 - size extended attributes
rt2x00 : Fix memleak in tx () path
ssb : Fix error when V1 SPROM extraction is forced
ath10k : make sure to use passive scan when n_ssids is 0
Btrfs : ignore non - FS inodes for send / receive
bnx2x : Prevent " Unknown MF " print in SF mode
drm : Revert " drm : Reject modes with more than 1 stereo flags set "
[ SCSI ] pmcraid : reject negative request size
ARM : tegra : fix U16 divider range check
drivers / staging / hv / blkvsc_drv . c : eliminate NULL pointer dereference
drm / i915 : Initialise g4x watermarks for disabled pipes
staging : gdm72xx : Condense two statements into one to improve code readability .
be2net : initialize netdev -> perm_addr
dma / timberdale : simplify conditional
sched / balancing : Fix ' local -> avg_load > sds -> avg_load ' case in calculate_imbalance ()
KVM : Really remove a slot when a user ask us so
mlx4_en : Fix read buffer overflow in mlx4_en_complete_rx_desc ()
drm / i915 : Check the framebuffer offset
rt2x00 : Fix memleak when RTS / CTS fails
staging : ozwpan : Check for correct config number .
ipvs : WRR scheduler does not need GFP_ATOMIC allocation
[ media ] cx25821 - audio - upstream . c : Move operators
sys_sync (): fix 16 % performance regression in ffsb create_4k test
sfc : Read - to - clear LM87 alarm / interrupt status at start of day
iscsi - target : return - EINVAL on oversized configfs parameter
drm / vmwgfx : Fix nasty write past alloced memory area
ARM : prevent VM_GROWSDOWN mmaps extending below FIRST_USER_ADDRESS
[ IPV6 ] ADDRCONF : Register inet6_dev earlier .
hwmon : ( f75375s ) Disable setting DC fan control mode for F75373
HWPOISON : Fix page count leak in hwpoison late kill in do_swap_page
ath5k : validate mode in ath5k_ani_init () before trying to set it
dlm : tcp_connect_to_sock should check for - EINVAL , not EINVAL
batman - adv : Fix mem leak in the batadv_tt_local_event () function
ath9k : set MAC version for AR9550
usb : core : check for valid id_table when using the RefId feature
i386 : Handle P6s without performance counters in nmi watchdog
ath9k : check for NULL rate when using ieee80211_get_rts_cts_rate ()
ixgbe : support software timestamping
KVM : ioapic : fix assignment of ioapic -> rtc_status . pending_eoi ( CVE - 2014 - 0155 )
vfio : fix crash on rmmod
drm / i915 : fix inconsistent brightness after resume
vxge : Update driver_config -> vpath_per_dev for each function in probe .
usb : dwc3 : gadget : add a debugging print when initializing endpoints
perf evsel : Fix uninitialized memory access to struct perf_sample
UBI : init_volumes : Ignore volumes with no LEBs
ceph : avoid null deref in osd request error path
ASoC : wm5110 : Correct DSP4R Mixer control name
drm / i915 : Fix missing return warning for ! CONFIG_DEBUGFS
iwlagn : overwrite EEPROM chain setting for 6250 devices
Input : turbografx - fix potential out of bound access
xprtrdma : Back off rkey when FAST_REG_MR fails
[ media ] cx24120 : Add missing command to cx24120_check_cmd
mac80211 : fix rx -> key NULL pointer dereference in promiscuous mode
USB : cyberjack : fix buggy integer overflow test
crypto : scatterwalk - Avoid flush_dcache_page on slab pages
x86 , tsc : Fix cpufreq lockup
[ SPARC64 ]: Fix loop termination in mark_kpte_bitmap ()
ARM64 : early_printk : Fix check for CONFIG_ARM64_64K_PAGES
[ media ] v4l : vb2 - dma - contig : align buffer size to PAGE_SIZE
pinctrl : handle zero found dt pinconfig properties better
tcp_yeah : don ' t set ssthresh below 2
/ proc / vmcore : fix seeking
mm : fix lost kswapd wakeup in kswapd_stop ()
thermal : core : ignore invalid trip temperature
mtd : mtd_blkdevs : don ' t increase ' open ' count on error path
[ PATCH ] zd1211rw : Add ID for Belkin F5D7050 v4000
i2o : fix overflow of copy_to_user ()
Input : gamecon - clear unused function pointers
KVM : async_pf : Add missing call for async page present
[ Bug 760 ] Fix setting WEP key in monitor mode causes IV lost .
dmaengine : ste_dma40 : Declare memcpy config as static
xhci : Handle HS bulk / ctrl endpoints that don ' t NAK .
mmc : atmel - mci : r / w proof capability only available since v2xx
gpio : Fix range check in of_gpio_simple_xlate ()
kvm / x86 : Remove Hyper - V SynIC timer stopping
crypto : mxs - dcp - fix type of ret for wait_for_completion_timeout
mwl8k : fix NULL pointer dereference on receive out - of - memory
drm / amdgpu : fix bug of vm_bo_map ( v2 )
sparc : semtimedop () unreachable due to comparison error
perf machine : Fix leak of ' struct thread ' on error path
ARM : EXYNOS : Recognize Samsung MFC v8 devices
at86rf230 : cleanup and squash stack variable
[ PATCH ] Fix RLIMIT_RTPRIO breakage
drm / nouveau / bios : check for null script pointers in parser
drivers / video / backlight / adp8870_bl . c : add missed props . type conversion
tracing : stop stack trace on first empty entry
tg3 : 5785 : Set port mode to MII when link down
USB : rtl8150_disconnect () needs tasklet_kill ()
drm / vmwgfx : Fix uninitialized return in vmw_cotable_unbind ()
staging : vt6655 : buffer overflow in ioctl
sh_eth : Fix memleak in sh_mdio_release
ext3 : Truncate allocated blocks if direct IO write fails to update i_size
x86 : tsc_64 . c make constant UL
Staging : comedi : integer overflow in do_insnlist_ioctl ()
[ SCSI ] mpt2sas : remove extraneous sas_log_info messages
hwmon / smsc47b397 : Don ' t report missing fans as spinning at 82 RPM
cfg80211 : DFS check dfs_region before usage
mmc : mxs - mmc : fix error return code in mxs_mmc_probe ()
timer : Convert kmalloc_node (... GFP_ZERO ...) to kzalloc_node (...)
microblaze : rt_sigreturn is too trigger - happy about sigaltstack errors
[ SCSI ] iscsi : nodelay fix
iio : Accept a leading '+' sign when parsing fixed point numbers
irq : check domain hwirq range for DT translate
ALSA : compress_core : integer overflow in snd_compr_allocate_buffer ()
slob : fix free block merging at head of subpage
mm : vmalloc allocator off by one
drm / i915 : The sprite scaler on Ironlake also support YUV planes
iwlwifi : mvm : make sure AP is operating for ToF
[ PATCH ] eCryptfs : dput () lower d_parent on rename
fbcon : bgcolor fix
powerpc / ipic : introduce missing kfree
mac80211 : make sure hw_key exists before checking its flags
kmemleak : Fix the kmemleak tracking of the percpu areas with ! SMP
l2tp : fix a potential skb leak in l2tp_xmit_skb ()
[ PATCH ] moxa serial : add proper capability check
drm / exynos : check crtc ' s dpms mode at SetCrtc
[ media ] dvb_demux : fix deadlock in dmx_section_feed_release_filter ()
USB : serial : add special case for processing of empty read urbs
be2iscsi : add an missing goto in error path
[ SCSI ] iscsi_boot_sysfs : Fix a memory leak in iscsi_boot_destroy_kset ()
[ PATCH ] inotify : fix idr_get_new_above usage
[ PATCH ] savagefb : Fix kfree before use
drm / i915 : mark display port DPMS state as ' ON ' when enabling output
kexec : fix omitting offset in extended crashkernel syntax
Btrfs : make error return negative in btrfs_sync_file ()
libceph : remove redundant declaration
ASoC : rsnd : rsnd_dai_connect () returns error if it connect to existing mod
Btrfs : Fix a crash when mounting a subvolume
sound / soc / fsl / p1022_ds . c : add missing of_node_put
KVM : MMU : Fix cleaning up the shadow page allocation cache
dm snapshot : lock snapshot while supplying status
efi : rtc - efi : Mark UIE as unsupported
x86 / PCI : make sure _PAGE_IOMAP it set on pci mappings
watchdog : gef_wdt . c : add missing remove callback
ovl : fix open in stacked overlay
cgroup : use release_agent_path_lock in cgroup_release_agent_show ()
ath9k_htc : check for underflow in ath9k_htc_rx_msg ()
irda : off by one
ALSA : cs4232 : fix crash during chip PNP detection
tpm : Do not dereference NULL pointer if acpi_os_map_memory () fails .
sdio : kick the interrupt thread upon a resume
staging : binder : fix usage of uninit scalar in binder_transaction ()
ipv6 - multicast : Fix memory leak in input path .
OpenRISC : Export dump_stack ()
x86 / boot - image : Don ' t leak phdrs in arch / x86 / boot / compressed / misc . c :: Parse_elf ()
x86 / irq : Fix move_irq_desc () for nodes without ram
suspend : switch the Asus Pundit P1 - AH2 to old ACPI sleep ordering
drm / i915 : Permit pinning whilst the device is ' suspended '
[ SCSI ] be2iscsi : fix bad if expression
perf bench : Fix NULL pointer dereference in " perf bench all "
staging : ft1000 : dont cast void * from kmalloc ()
arm : kvm : fix CPU hotplug
gianfar : Check if phydev present on ethtool - A
ide : unexport ide_hwifs []
[ PATCH ] remove unnecessary blk_queue_bounce in SG_IO
xhci : fix off by one error in TRB DMA address boundary check
sctp : Clear fast_recovery on the transport when T3 timer expires .
drm / cirrus : Constify function pointer structs
fs / ramfs / file - nommu . c : replace count * size kzalloc by kcalloc
btrfs : reada : move reada_extent_put to place after __readahead_hook ()
staging : vt6656 : don ' t return zero on failure path in vt6656_probe ()
[ media ] media - device : fix infoleak in ioctl media_enum_entities ()
ethtool : time to blink provided in seconds not jiffies
[ SCSI ] Return NEEDS_RETRY for eh commands with status BUSY
staging , media , easycap : Fix mem leak in easycap_usb_probe ()
econet : Fix econet_getname () leak
hugetlbfs : fix i_blocks accounting
i40e : Increase PF reset max loop limit
ath : Fix NULL ptr dereference in ath_reg_apply_world_flags
sigtimedwait : use freezable blocking call
ir - core : Fix the delete logic
brcmfmac : Fix crash on cleanup .
[ TG3 ]: Update version to 3 . 85
staging : iio : adc : max1363 : prevent buffer overflow
KVM : Force vmexit with virtual interrupt delivery
xhci : fix sparse warning in xhci - trace . h
usb : phy : mv - otg : use to_delayed_work instead of cast
perf : Fix NULL deref
sonice : Fix build due to botched netdev_alloc_skb () conversion .
af_packet : remove unnecessary BUG_ON () in tpacket_destruct_skb
sh : Account for INITIAL_JIFFIES when using jiffies clocksource .
crypto : ccm - Fix handling of null assoc data
drivers / cdrom / gdrom . c : fix device number leak
perf : Fix error return code
staging : vt6656 : turn radio off after mac registered .
perf tools : Fix TUI helpline output
ieee1394 : sbp2 : prepare for s / g chaining
[ PATCH ] scsi_error thread exits in TASK_INTERRUPTIBLE state .
[ SCSI ] aacraid : fix potential panic in thread stop
mac80211 : fix PS vs . scan race
chipidea : error on overflow for port_test_write
ARM : 8544 / 1 : set_memory_xx fixes
[ SCSI ] scsi_lib : Set the device state from transport - offline to running
radeon_display : Use pointer return error codes
drm / i915 : Missed lock change with rps lock
ata : fix sparse warning in ata_piix . c
usb : host : xhci : Fix Null pointer dereferencing with 71c731a for non - x86 systems
ixgbe : fix for race with 8259 ( 8 | 9 ) during shutdown
x86 / smpboot : Move data structure to its primary usage scope
iwlwifi : mvm : don ' t allow sched scans without matches to be started
staging : iio : adis16400 : Remove samplerate_available attribute
staging : iio : iio_ring_rip_outer return immediately if rip_lots returns <= 0
tg3 : Turn off ASF " driver alive " heartbeats for APE
[ media ] zoran : fix compiler warning
intel_sst : ignore IRQ when suspended
[ POWERPC ] Fix harmless typo
[ XFRM ]: Fix missed error setting in xfrm4_policy . c
omap_hsmmc : add missing probe handler hook
[ SCSI ] scsi_transport_iscsi : make priv_sess file writeable only by root
KVM : SVM : Advance instruction pointer in dr_intercept
dt : protect against NULL matches passed to of_match_node ()
UDF : Close small mem leak in udf_find_entry ()
leds : Make header variable naming consistent
ASoC : Revert " ASoC : mc13783 : Provide codec -> control_data "
staging : iio : isl29018 : fix sparse warning regarding incorrect type ( different signedness )
ov7670 : clean up ov7670_read semantics
mtd : mtdchar : add missing initializer on raw write
HID : corsair : fix mapping of non - keyboard usages
[ PATCH ] fix oddball boolean logic in s390 netiucv
bnx2x : Force fundamental reset for EEH recovery
[ NETFILTER ]: recent match : missing refcnt initialization
drm / i915 : intel_frontbuffer_flip_prepare () doesn ' t need struct_mutex
befs : fix leak
[ XFS ] Wait for all I / O on truncate to zero file size
ALSA : hda - Don ' t refer ELD when unplugged
dm mpath : do not fail paths after integrity errors
[ media ] m5mols : Fix bug in stream on handler
e100 : make sure vlan support isn ' t advertised on old adapters
Btrfs : check return value of btrfs_search_slot properly
ALSA : hdsp : silence and underflow warning
ALSA : hda - Detect non - SPDIF digital I / O
mfd : sec - core : Enable RTC on S2MPS11 PMIC
[ PATCH ] scsi : device_reprobe () can fail
regmap : Fix ' ret ' would return an uninitialized value
ipv4 : Limit size of route cache hash table
mx3fb : fix regression with uninitalized fb_info -> mm_lock mutex
dmaengine : i . MX : Fix merge of cookie branch .
cifs : integer overflow in parse_dacl ()
xfrm6 : release dev before returning error
perf stat : better error message for unsupported events
jfs : fix d_revalidate oopsen on NFS exports
iio : buffer : Free active scan mask in iio_disable_all_buffers ()
drm / i915 : Add a couple WARN ()' s to catch missing locks
rtc : Fix ioctl error path return
uprobes : Allocate -> utask before handler_chain () for tracing handlers
hwmon : ( emc1403 ) Remove unnecessary hwmon_device_unregister
ASoC : Check return value of strict_strtoul () in WM8962
cpufreq : pcc : call CPUFREQ_POSTCHANGE notfier in error cases
ALSA : pcm : don ' t override timestamp unconditionally
AFS : Potential null dereference
fuse : fix retrieve length
misc : genwqe : Fix potential memory leak when pinning memory
xfs : prevent leaking uninitialized stack memory in FSGEOMETRY_V1
[ ARM ] fix leak in iop13xx / pci
[ PATCH ] Char : tty , delete wake_up_interruptible after tty_wakeup
kernel / sys . c : fix missing rcu protection for sys_getpriority ()
drm / i915 : mark a newly - created GEM object dirty when filled with data
[ XFS ] Fix a mutex_destroy diagnostic about a locked - mutex - on - destroy from
drm / ttm : Don ' t clear page metadata of imported sg pages
xfs : check if device support discard in xfs_ioc_trim ()
f2fs : report - ENOENT for unreached data indices
vmwgfx : return - EFAULT if copy_to_user fails
amd - xgbe - phy : Fix build break for missing declaration
brcmfmac : initialize escan function pointer during scheduled scan
ocfs2 : copy fs uuid to superblock
irqchip : irq - crossbar : Not allocating enough memory
[ BOND ]: Fix warning in bond_sysfs . c
dmaengine : mv_xor : fix big endian operation in register mode
mmc : omap_hsmmc : Update error code for response_busy cmd
staging : comedi : ni_660x : use comedi_handle_events ()
headers_check fix : linux / bfs_fs . h
sfc : Use kzalloc () to ensure struct efx_spi_device is fully initialised
Btrfs : free tmp ulist for qgroup rescan
usb : ehci - mxc : check for pdata before dereferencing
drm / i915 : WARN if the bios reserved range is bigger than stolen size
[ PATCH ] IB / mthca : allocate correct number of doorbell pages
cfq - iosched : Respect ioprio_class when preempting
USB : fix interface sysfs file - creation bug
mm : check that we have the right vma in __access_remote_vm ()
Input : pmic8xxx - pwrkey - pass correct pointer to free_irq ()
gpio - mcp23s08 : correctly handling failed allocation
xtensa : fix sysmem reservation at the end of existing block
writeback : laptop_mode_timer_fn () needs rcu_read_lock () around bdi_writeback iteration
hfsplus : use - ENOMEM when kzalloc () fails
platform - drivers - x86 : intel_menlow : add missing return AE_OK for intel_menlow_register_sensor ()
[ PATCH ] zd1211rw : Add ID for Sitecom WL - 162
drivers / net / wan : introduce missing kfree
mm , percpu : Make sure percpu_alloc early parameter has an argument
leds : Fix trigger unregister_simple if register_simple fails
x86 : BUILD_IRQ say . text to avoid . data . percpu
ASoC : 88pm860x - codec : Cleanup manual bias level transitions
[ PATCH ] compat_sys_nfsservctl (): handle errors correctly
tpm_tis : free irq after probing
pinctrl : baytrail : add missing module removal support
perf tools : Try to lookup kernel module map before creating one
drm / nouveau : No need to force evict = true when swapping evicted BOs back in .
ext4 : add missing kfree () on error return path in add_new_gdb ()
ab8500 - fg : Use correct battery charge full design
rtc - at91rm9200 : add match - table compile guard
staging : hfi1 : sdma : Use setup_timer
[ SCSI ] ibmvfc : Fix invalid error response handling
mwl8k : Disable tx_wait completion
dm raid1 : fail writes if errors are not handled and log fails
drm / i915 / crt : Flush register prior to waiting for vblank .
USB : musb : forward debug mode feature to gadget
crypto : pcrypt - Use crypto_aead_set_reqsize helper
clk : shmobile : div6 : Avoid division by zero in . round_rate ()
ceph : fix ack counter reset on connection reset
fs_enet : Fix a memory leak in fs_enet_mdio_probe
tty : xuartps : Fix " may be used uninitialized " build warning
hub : debug message for failing to enable device
drm / i915 / sdvo : Cleanup connector on error path
iio : accel : bmc150 - accel : fix counting direction
perf kmem : Fix compiler warning about may be accessing uninitialized variable
[ XFS ] fix valid but harmless sparse warning
esas2r : fix an error path in esas2r_ioctl_handler
raid5 : disable batch with log enabled
fuse : reset waiting
dmaengine : sirf : off by one in of_dma_sirfsoc_xlate ()
ASoC : rt5640 : add default case for unexpected ID
hwmon : ( ntc_thermistor ) Ensure that data -> name string is terminated
staging : unisys : fix spacing in parahotplug_request_create
ath9k : Fix rx filtering issue for older chips
mac80211 : Fixing slow IBSS rejoin
[ PATCH ] bcm43xx : Fix deviation from specifications in set_baseband_attenuation
acer - wmi : support Lenovo ideapad S205 1038DPG wifi switch
ASoC : s6000 : Fix unlocked snd_pcm_stop () call
ACPI : Change package length error to warning
Staging : vt6656 : fix the memory free bug in vntwusb_disconnect ()
UBI : add missing kmem_cache_free () in process_pool_aeb error path
ALSA : pcm : optimize xrun detection in no - period - wakeup mode
[ XFRM ]: Fix possible overflow of sock -> sk_policy
genirq : NULL struct irq_desc ' s member ' name ' in dynamic_irq_cleanup ()
ALSA : bebob : Fix a missing to unlock mutex in error handling case
drivers : net : cpsw : irq not disabled in cpsw isr in particular sequence
drm / i915 : Skip stolen region initialisation if none is reserved
staging : comedi : do not return - ENOSYS .
drm : sti : Fix return value check in sti_drm_platform_probe ()
cgroup : fix an off - by - one bug which may trigger BUG_ON ()
audit : Handle embedded NUL in TTY input auditing
amd8111e : Don ' t receive packets when the napi budget == 0
um : don ' t leak floating point state and segment registers on execve ()
[ SCSI ] bnx2i : Added reconnect fix connecting against Lefthand targets
ipmi : Clear drvdata when interface is removed
pkt_sched : sch_generic : Add Qdisc_ops peek () method .
ath9k : skip ATH9K_INT_TIM_TIMER when we are idle
KVM : x86 emulator : VM86 segments must have DPL 3
drivers / rtc / rtc - pcf2123 . c : fix error return code in pcf2123_probe ()
usb : musb : gadget : clear gadget_driver when gadget is stopped
staging / zcache : Fix memory leak while dropping an o2net_sock_container object
clk : versatile : free icst on error return
clk : divider : return real rate instead of divider value
drm / radeon : check kmalloc () for failures
UBI : bugfix : dont oops with NULL module parameter
ocfs2 : check if cluster name exists before deref
mtd : nand : mxc_nand : Check the return value from clk_prepare_enable ()
spi : mediatek : remove clk_disable_unprepare ()
svcrpc : make svc_create_xprt enqueue on clearing XPT_BUSY
usb : chipidea : udc : . pullup is valid only when vbus is there
mm , hugetlb : add VM_NORESERVE check in vma_has_reserves ()
pcmcia : rsrc_nonstatic : check value , not pointer
iommu / amd : Fix BUG when faulting a PROT_NONE VMA
x86 apic_32 . c section fix
drm / i915 : synchronize_irq () before turning off disp2d power well on VLV / CHV
kvm : irqchip : fix memory leak
gianfar : Fix missing sock reference when processing TX time stamps
Input : turbografx - clear unused function pointers
Btrfs : fix race between allocate and release extent buffer .
Input : twl4030_keypad - pass correct pointer to free_irq ()
ath9k : Remove unused rx_edma in ath_rx_addbuffer_edma ()
gpio : max730x : eliminate double free
leds : turn the blink_timer off before starting to blink
vm : fix incorrect unlock error path in madvise_free_huge_pmd
ath9k : No need for that extra memcpy
mmc : mxcmmc : handle mmc_of_parse () errors during probe
sched : Fix incorrect sanity check
ALSA : hda - Add MSI blacklist
staging : netlogic : Fix multiple assignments
sysctl : Support vdso_enabled sysctl on SH .
iio : core : Handle error when mask type is not separate
Input : edt - ft5x06 - ignore touchdown events
staging : comedi : ni_nio_common : don ' t write non - existing caldac ' s
staging / rdma / hfi1 : Don ' t attempt to qualify or tune loopback plugs
staging : mt29f_spinand : Export OF module alias information
[ POWERPC ] Fix powermac late initcall to only run on powermac
[ media ] sony - btf - mpx : Drop needless newline in param description
wl12xx : minor fix in sched_scan_ssid_list
uselib : add missing MNT_NOEXEC check
wlcore : save last regdom configuration on stop
drm / nouveau / pm : don ' t read / write beyond end of stack buffer
[ ALSA ] hda - code : Fix the array size of codec list
libata : initialize qc -> dma_dir to DMA_NONE
i40e : Add missing braces to i40e_dcb_need_reconfig ()
ALSA : hda - Skip ELD notification during PM process
KVM : PPC : Add emulation for dcba
clocksource : of : Respect device tree node status
PCI : Allow a NULL " parent " pointer in pci_bus_assign_domain_nr ()
[ PATCH ] knfsd : nfsd4 : fix open permission checking
ext4 : don ' t save the error information if the block device is read - only
ssb : Fix error when V1 SPROM extraction is forced
cxl : Reset default context for vPHB on release
perf tools : Add missing closedir in multi tracepoint processing
PCI : Mark 64 - bit resource as IORESOURCE_UNSET if we only support 32 - bit
GFS2 : Fix a use - after - free that coverity spotted
vfs : fix propagation of atomic_open create error on negative dentry
KVM : destroy workqueue on kvm_create_pit () failures
drm / radeon / kms : Fix NULL ptr dereference
staging : iio : hmc5843 : Set iio name property in sysfs
dmaengine : ste : use DMA_COMPLETE for dma completion status
ath10k : fix error handling in pci_probe
btrfs : Fix memory leak in btrfs_read_fs_root_no_radix ()
[ SCSI ] qla2xxx : Perform firmware dump procedure on mailbox command timeout .
drm / i915 : Reset vma -> mm_list after unbinding
[ IPSEC ] Set byid for km_event in xfrm_get_policy
[ PATCH ] PCI Hotplug : shpchp driver doesn ' t program _HPP values properly
[ media ] media : davinci : vpif_display : drop unused member fbuffers
staging : lustre : prevent underflows in lnet_parse_reply ()
ethtool : Fix potential kernel buffer overflow in ETHTOOL_GRXCLSRLALL
caif - hsi : Add missing return in error path
[ media ] radio - aimslab . c needs # include < linux / delay . h >
qeth : keep ip - address after LAN_OFFLINE failure
drivers / staging / cx25821 / cx25821 - video . c : introduce missing kfree
ext4 : remove dynamic array size in ext4_chksum ()
ALSA : hda / generic - Check power state cap at updating the widget power
[ S390 ] idle : avoid RCU usage in extended quiescent state
Btrfs : don ' t panic if orphan item already exists
Btrfs : fix scheduler warning when syncing log
m68k : export atari_keyb_init
scsi : storvsc : fix SRB_STATUS_ABORTED handling
powerpc : kmalloc failure ignored in vio_build_iommu_table ()
staging : vt6656 : check ieee80211_bss_conf bssid not NULL
cfg80211 : off by one in nl80211_trigger_scan ()
[ PATCH ] ext3 : Wrong return value for EXT3_IOC_GROUP_ADD
x86 : fix warning on 32 - bit non - PAE
KVM : PIT : control word is write - only
mac80211 : tx , use dev_kfree_skb_any for beacon_get
devres : WARN () and return , don ' t crash on device_del () of uninitialized device
fix wrong argument of tc35815_read_plat_dev_addr ()
cfq - iosched : make seek_mean converge more quickly
[ IA64 ] set_thread_area fails in IA32 chroot
ARM : shmobile : r8a7794 : Reserve memory as other R - Car Gen2 SoCs
[ PATCH ] coverity : tty_ldisc_ref return null check
[ PATCH ] fix creating zero sized bio mempools in low memory system
ACPICA : Fix for memory leak related to DdbHandle objects
drivers / net / hamradio / 6pack . c : move a dereference below a NULL test
[ media ] uvcvideo : Fix a " ignoring return value of ‘ __clear_user ’" warning
ALSA : make sparc / dbri . c : snd_dbri_proc () static
[ MTD ] mtd / ofpart . c : add MODULE_LICENSE
[ media ] vb2 : fix compiler warning
drivers : sh : Enable PM runtime for new R - Car Gen2 SoCs
[ PATCH ] Fix sys_migrate_pages : Move all pages when invoked from root
[ PATCH ] ppc64 : lparconfig . c memory leak
[ SCSI ] qla2xxx : Process DPC requests within valid Fabric topologies .
wlcore : initialize per - link FW freed blocks correctly
parisc : isa - eeprom - Fix loff_t usage
staging : comedi : ni_at_ao : fix namespace clash of ' RSI ' define
mm : cma : fix condition check when setting global cma area
w1 : fix for loop exit condition in w1_seq_show ()
firewire : core : remove an unnecessary zero initialization
hso : Add new product ID
ALSA : als4000 : enable burst mode
tcp : Fix tcp_mark_head_lost () with packets == 0
ALSA : hda - Update automute / automic upon jack retasking
Staging : comedi : fix memory leak in comedi_bond . c
Staging : wlan - ng : memsetting the wrong amount of data
drm / i915 : fix dmabuf vmap support
drm : fixes when i2c encoder slave mode_fixup is null .
[ XFS ] Fix lock inversion in forced shutdown .
ocfs2 : unmap_mapping_range () in ocfs2_truncate ()
[ media ] uvcvideo : Constify the uvc_entity_match_guid arguments
cpumask : Use nr_cpu_ids in seq_cpumask
md : check for memory allocation failure in faulty personality
[ ARM ] 4557 / 1 : Fix PXA irq gpio initialization
[ ALSA ] ASoC - Bit clock matching error
r8169 : balance pci_map / pci_unmap pair
sh : force dcache flush if dcache_dirty bit set .
[ media ] w9966 : zero device state after a detach
eeepc - laptop : store_cpufv : return error if set_acpi fails
[ media ] media : omap3isp : fix a potential NULL deref
KVM : nVMX : Report 2MB EPT pages as supported
USB : Fix option_ms regression in 2 . 6 . 31 - rc2
ALSA : hdsp - Ignore MIDI and PCM events in interrupts until initialized
usb : dwc3 : gadget : ignore endpoint IRQs when endpoint is disabled
ceph : fix rare potential cap leak
e1000e : magic number cleanup - ETH_ALEN
[ XFS ] Check return value of xfs_buf_get_noaddr ()
bridge : Clear IPCB before possible entry into IP stack
staging : comedi : das16m1 : use comedi_buf_write_samples ()
perf thread : Allow deletion of a thread with no map groups
i40e : validate VSI param from VFs
ACPI : Fix crash on ASUS laptops
mm : alloc_large_system_hash check order
sctp : disable max_burst when the max_burst is 0
[ PATCH ] hrtimer : validate timespec of do_sys_settimeofday
perf tools : Set vmlinux_path__nr_entries to 0 in vmlinux_path__exit
staging : gdm72xx : underflow in netlink_rcv_cb ()
staging / slicoss : disable pci device at remove
[ PATCH ] hrtimer : call get_softirq_time () only when necessary in run_hrtimer_queue ()
efivarfs : Never return ENOENT from firmware
xfs : suppress spurious uninitialised var warning in xfs_bmapi ()
be2net : take care of __vlan_put_tag return value
mac80211 : fix ie memory allocation for scheduled scans
[ media ] pctv452e : Replace memset with eth_zero_addr
[ PATCH ] spufs fix spu_acquire_runnable error path
drm / nvc0 / ltcg : mask off intr 0x10
[ media ] cx25821 : do not expose broken video output streams
drivers / scsi / aacraid / commctrl . c : fix mem leak in aac_send_raw_srb ()
[ PATCH ] make kernel / printk . c : ignore_loglevel_setup () static
ALSA : rme32 . c irq enabling after spin_lock_irq
drm / r600 : fix memory leak introduced with 64k malloc avoidance fix .
USB : pl2303 : fix return value of tiocmset
perf : Fix perf_event_init_context ()
ath10k : fix num_legacy_stations tracking
x86 : fix runtime error in arch / x86 / kernel / cpu / mcheck / mce_amd_64 . c
ipv4 : netfilter : ip_tables : fix information leak to userland
USB : EHCI : add check for wakeup / suspend race
i386 : fix NMI watchdog not reserving its MSRs
xfs : ensure we capture IO errors correctly
crypto : cts - Weed out non - CBC algorithms
fuse : Initialize total_len in fuse_retrieve ()
sunrpc : fix rpc debugging
mac80211 : Ensure initializing private mc_list in prepare_multicast ().
ipv6 mcast : Check address family of gf_group in getsockopt ( MS_FILTER ).
sh_eth : Check for DMA mapping errors on transmit
sound : snd_ctl_elem_add : fix value count check
drivers / w1 / w1_int . c : multiple masters used same init_name
mtd : lart : initialize writebufsize
drm / radeon : unlock the ring mutex while waiting for the next fence
[ PATCH ] ppc32 : don ' t call progress functions after boot
ixgbe : Deny MACVLAN requests from VFs with admin set MAC
ALSA : hda - Fix digital mic on dell - m4 - 1 and dell - m4 - 3
[ media ] media : am437x - vpfe : return error in case memory allocation failure
Btrfs : stop leaking btrfs_bios on readahead
[ media ] DVB : Set scanmask for Budget / SAA7146 cards
ASoC : Check for NULL register bank in snd_soc_get_cache_val ()
btrfs : fix null pointer dereference in btrfs_show_devname when name is null
regmap : Don ' t try to map non - existant IRQs
tracepoints : use rcu_ * _sched_notrace
drbd : Fix module refcount leak in drbd_accept ()
ipv6 : Assume the loopback address in link - local scope .
bpf : avoid copying junk bytes in bpf_get_current_comm ()
perf ui / tui : Print helpline message as is
[ PATCH ] e1000 : narrow down the scope of the tipg timer tweak
cfq - iosched : fix no - idle preemption logic
drm / i915 : take struct_mutex in i915_dma_cleanup ()
powerpc / pseries : Fix possible leaked device node reference
ARM : LPC32xx : serial . c : Fixed loop limit
rtlwifi : rtl8192cu : Fix unitialized struct
userns : Fix posix_acl_file_xattr_userns gid conversion
ch7006 : correctly handling failed allocation
i2c : i2c - pnx : Limit minimum jiffie timeout to 2
at86rf230 : add force slotted operation bit
USB : musb_gadget_ep0 : fix unhandled endpoint 0 IRQs , again
Input : usbtouchscreen - add support for LG Flatron T1710B
mfd : Fix palmas regulator pdata missing
[ NETFILTER ]: nf_conntrack_sip : clear address in parse_addr ()
uvesafb : bitwise OR has higher precedence than ?:
mac80211 : initialize sta -> last_rx in sta_info_alloc
batman - adv : Remove space before semicolon
ocfs2 : fix warning in ocfs2_file_aio_write ()
amd - xgbe : Check for complete packet on skb allocation error
qlcnic : Fix potential NULL dereference
ARM : EXYNOS : Fix potential NULL pointer access in exynos_sys_powerdown_conf
cfg80211 : fix elapsed_jiffies calculation
uas : Clear cmdinfo on command queue - ing
[ POWERPC ] allocation fix in ppc / platforms / 4xx / luan . c
[ media ] usbtv : correctly handling failed allocation
block : move timeout field in struct request to pack better
drm / nouveau : fix memory leak
[ PATCH ] tpm : fix memory leak
drm / ast : Use lockless gem BO free callback
[ SCSI ] qla2xxx : Ensure RISC - interrupt - enabled consistency for IS_NOPOLLING_TYPE () ISPs .
powerpc / pseries : Fix of_node_put () underflow during DLPAR remove
tty / serial : at91 : fix race condition in atmel_serial_remove
[ PATCH ] audit : Moved variable declaration to beginning of function
fs / binfmt_misc . c : add terminating newline to / proc / sys / fs / binfmt_misc / status
ocfs2 : fix check for locally granted state during dlmunlock ()
isp1760 - hcd : use USB_DT_HUB
drm / radeon : fix UVD 256MB check
mm : highmem : export kmap_to_page for modules
ASoC : fix uninitialised variable in siu_dai . c
dm : bump the ioctl version to 4 . 32 . 0
crypto : qat - Prevent dma mapping zero length assoc data
drm / i915 : Restore lost DPLL register write on gen2 - 4
bonding : use l4 hash if available
[ NETFILTER ]: nfnetlink_queue : fix computation of allocated size for netlink skb .
jffs2 : Add missing capability check for listing trusted xattrs
Input : synaptics - do not retrieve the board id on old firmwares
perf intel - pt : Fix off - by - one comparison on maximum code
kconfig / nconf : prevent segfault on empty menu
ext4 : add check for inodes_count overflow in new resize ioctl
KVM : ppc : deliver INTERRUPT_FP_UNAVAIL to the guest
pch_uart : add spin_lock_init
btrfs : Fix the bug that fs_info -> pending_changes is never cleared .
UDF : Fix a null pointer dereference in udf_sb_free_partitions
stm class : Prevent user - controllable allocations
ath9k : Fix a crash in 2 WIRE btcoex chipsets
Btrfs : set truncate block rsv ' s size
airo : return from set_wep_key () when key length is zero
xfs : Fix possible use - after - free with AIO
[ LLC ]: Restrict LLC sockets to root
dm : swap target postsuspend call and setting suspended flag
i2c / scx200_acb : Don ' t use 0 as NULL pointer
ALSA : line6 : fix ifnullfree . cocci warnings
wl12xx : protect wl12xx_op_set_rts_threshold ()
[ SCSI ] qla2xxx : Pad IOCB structure for size requirements .
tick / nohz : Fix possible missing clock reprog after tick soft restart
Btrfs : fix regression of btrfs device replace
drm / i915 : Correctly reject invalid flags for wait_ioctl
bridge : vlan : fix usage of vlan 0 and 4095 again
HID : wacom : fix proximity tool release
mmc : dw_mmc : fix the wrong condition checking
[ media ] coda : add compressed flag to format enumeration output
tcp : fastopen : fix rcv_wup initialization for TFO server on SYN / data
HID : extend mask for BUTTON usage page
tulip : Properly check dma mapping result
ext4 : online defrag is not supported for journaled files
JFS : FIx one more plain integer as NULL pointer warning
Btrfs : return - 1 when lzo compression makes data bigger
[ ETH ]: Validate address in eth_mac_addr
soc : ti : qmss : fix the case when ! SMP
Btrfs : fix em leak in find_first_block_group
ASoC : Intel : Skylake : Move up pipe mem free
ARM : 6135 / 1 : mx21 / devices : fix USBOTG resource
igb : limit EEPROM access
virtio / 9p : verify device has config space
mm : numa : migrate : Set last_nid on newly allocated page
ipv4 : Use FIELD_SIZEOF () in inet_init ().
staging / rtl8192u - check for skb alloc failure
handle recursive calls to bust_spinlocks ()
Input : ads7846 - remove redundant regulator_disable call
tracing / function - graph - tracer : fix merge
TPM : tpm_nsc : Fix a double free of pdev in cleanup_nsc
cifs : Use BUILD_BUG_ON to validate uids and gids are the same size
isdn : guard against a potential NULL pointer dereference in old_capi_manufacturer ()
leds : tca6507 : silence an uninitialized variable warning
ocfs2_dlm : Missing get / put lockres in dlm_run_purge_lockres
gma500 : reversed test in mdfld_dbi_dsr_exit ()
x86 , kaslr : Use char array to gain sizeof sanity
eCryptfs : Flush dirty pages in setattr
mmc : unbreak sdhci - esdhc - imx on i . MX25
ALSA : hdspm - Constrain periods to 2 on older cards
floppy : Add an extra bound check on ioctl arguments
mmap : check -> vm_ops before dereferencing
KVM : arm64 : vgic - its : Fix vgic_its_check_device_id BE handling
ASoC : arizona : Record FLL setting when disabling
spi : fsl : Use kmalloc_array () in of_fsl_spi_get_chipselects ()
pinctrl : uniphier : fix . pin_dbg_show () callback
mmc : mmci : Cascade EPROBE_DEFER from regulators .
tools lib traceevent : Handle % z in bprint format
selinux : fix off - by - one in setprocattr
ath10k : enable Adaptive Noise Immunity ( ANI ) by default
[ media ] gspca / stv06xx_pb0100 : Don ' t potentially deref NULL in pb0100_start ()
ACPICA : debugger : fix memory leak on Pathname
drm / i915 : Use MLC ( l3 $) for context objects
ASoC : ml26124 : Fix negative array index read
drm / radeon : use fixed PPL ref divider if needed
cfg80211 : don ' t set privacy w / o key
mac80211 : fix TX aggregation TID struct leak
Btrfs : avoid memory leak of extent state in error handling routine
x86 : trim ACPI sleep stack buffer
drm / sti : in crtc_atomic_flush , enable only planes of this crtc
drm / radeon / kms : prefer high post dividers in legacy pll algo
[ media ] mceusb : plug memory leak on data transmit
xen / gntalloc : release grant references on page free
gpio : f7188x : fix edit mistake
drm / i915 : fix integer overflow in i915_gem_execbuffer2 ()
ARM : S3C6410 : update clk -> parent when setting clock source
drm / i915 : Only check for valid PP_ { ON , OFF } _DELAYS on pre ILK hardware
acm : Return ENODEV instead of EINVAL when trying to open ACM device .
b43 : stop format string leaking into error msgs
clk : ti : Check kzalloc () for failures
drm / i915 : Fix intel_tile_width () parameters
fsl / fman : double free on probe failure
i2c : i2c - designware - platdrv : Drop class based scanning to improve bootup time
[ ARM ] 5315 / 1 : Fix section mismatch warning ( sa1111 )
iommu / vt - d : Fix NULL pointer dereference in device_to_iommu
telephony : fix return value
bsg : fix bsg_poll () to return POLLOUT properly
drm / amdgpu / gfx9 : bypass clockgating setting
sky2 : Shut off interrupts before NAPI
ath9k : Remove pointless ath9k_ps_restore () in ath_detach ()
[ media ] radio - bcm2048 : fix missing unlock on error in bcm2048_rds_fifo_receive ()
drm / radeon / si : add dpm quirk for Oland
sfc : mark state UNINIT after unregister
Input : gpio_keys - fix a memory leak
AFS : Fix possible null pointer dereference in afs_alloc_server ()
drm / radeon / kms : although LVDS might be possible on crtc 1 don ' t do it .
mac80211 : allow channel change while mesh is down
greybus : call put_device () on error
[ XFRM ]: xfrm_migrate () needs exporting to modules .
ASoC : Serialize access to dapm_power_widgets ()
knfsd : Fixed problem with NFS exporting directories which are mounted on .
mac80211 : reduce the amount of unnecessary traffic on cooked monitor interfaces
[ PATCH ] null dereference in fs / jbd2 / journal . c
[ CIFS ] relinquish fscache cookie before freeing CIFSTconInfo
ALSA : es1688 : Use strcpy () instead of sprintf ()
KVM : s390 : fix signal handling
[ TG3 ]: Update version to 3 . 82 .
rose : fix info leak via msg_name in rose_recvmsg ()
perf symbols : Do not open device files
Staging : rtl8188eu / core : Coding style fix , set constant operand on right in tests
drm / ttm : fix incorrect logic in ttm_bo_io path
drivers / rtc / rtc - ds1307 . c : fix BUG shown with lock debugging enabled
ARM : pxa : magician : Fix wrongly enabled USB host ports
dm : sysfs skip output when device is being destroyed
USB : ftdi_sio : fix minor typo in get_ftdi_divisor
extcon : Fix return value in extcon - class . c
[ TCP ] tcp_probe : use GCC printf attribute
seccomp : cap SECCOMP_RET_ERRNO data to MAX_ERRNO
PCI : Prevent out of bounds access in numa_node override
x86 : e820 memmap - add checking for NULL early param
printk : suppress empty continuation lines
gpio / samsung : only register available gpio banks
[ media ] radio - maxiradio : fix memory leak when device is removed
[ JFFS2 ] Add declaration of jffs2_lzo_ { init , exit } to compr . h
cw1200 : Fix cw1200_debug_link_id
KVM : cleanup "* new . rmap " type
clk : rockchip : rename rga clock - id on rk3399
drm / sti : sti_vtg : Handle return NULL error from devm_ioremap_nocache
[ ARM ] pxa : fix pxa27x_udc default pullup GPIO
[ PATCH ] hwrng : fix intel probe error unwind
ipv6 : ignore null_entry in inet6_rtm_getroute () too
drm / i915 : Avoid NULL ctx -> obj dereference in debugfs / i915_context_info
Staging : rtl8821ae : Drop memory allocation cast for kmalloc in hal_btc . c
drm / i915 : Kill leftover GTIIR writes from valleyview_irq_preinstall ()
[ media ] au8522_common : add missing MODULE_LICENSE
bnx2x : correct advertisement of pause capabilities
[ SPARC64 ] sunhv : Support SYSRQ properly .
sfc : SFT9001 : Fix condition for LNPGA power - off
ath9k : fix 5 / 10MHz channel width initialization on ar9003
pm2fb : free cmap memory on module remove
x86 , ioapic : Throw BUG instead of NULL dereference
i40e : stop the service task at shutdown
powerpc / pmac : Fix SMP kernels on pre - core99 UP machines
xfs : fix might_sleep () warning when initialising per - ag tree
perf probe : Trivial typo fix for -- demangle
sched / headers : Remove the runqueue_is_locked () prototype
HID : cp2112 : fix to force single data - report reply
Staging : rtl8192e : Use ! x instead of x == NULL
ALSA : hda - fix read before array start
ocfs2 : o2hb : fix double free bug
mtd : atmel_nand : trivial : change DMA usage information trace
x86 - 32 : Fix sparse warning for the __PHYSICAL_MASK calculation
hfsplus : handle match_strdup failure
[ SCSI ] ibmvscsi : Correct DMA mapping leak
fb_defio : Push file_update_time () into fb_deferred_io_mkwrite ()
timers , drivers / tty / metag_da : Initialize the poll timer as pinned
sata - fsl : Apply link speed limits
HID : multitouch : force retrieving of Win8 signature blob
s390 / chsc : fix ioctl CHSC_INFO_CU command
mfd : Use devm_ * APIs for mc13xxx : remove leftover kfree
KVM : SVM : Set the ' busy ' flag of the TR selector
[ media ] V4L : mt9t112 : use after free in mt9t112_probe ()
ath5k : initialize default noise floor
xen / privcmd : print SIGBUS faults
ipvs : info leak in __ip_vs_get_dest_entries ()
ixgbe : add missing rtnl_lock in PM resume path
ALSA : usb : caiaq : check for cdev -> n_streams > 1
wext : verify buffer size for SIOCSIWENCODEEXT
greybus : greybus_protocol : fix order of sdio get caps response
usb : xhci : Fix OOPS in xhci error handling code
drm / radeon : force MSIs on RS690 asics
spi : add spi_message_init_no_memset to avoid zeroing the spi_message
xfrm_user : verify policy direction at XFRM_MSG_POLEXPIRE handler
ALSA : usb - audio : Fix quirks code is not called
gpio : GPIO_GET_CHIPINFO_IOCTL : Fix information leak
usb : gadget : storage : release superspeed descriptors .
[ PATCH ] Add deprecated_for_modules
amd64_edac : Fix potential memleak
[ media ] lgdt3306a : one bit fields should be unsigned
pinctrl : single : fix error return code in pcs_parse_one_pinctrl_entry ()
powerpc / pci : fix PCI - e check link issue
[ media ] dvb_usb_v2 : fix error handling for . tuner_attach ()
ASoC : Intel : add support for Realtek 5651 on Cherrytrail
HID : egalax : Use kzalloc
mac80211 : fix potentially redundant skb data copying
bcma : support chipsets with PMU and GCI cores ( devices )
KVM : x86 : race between KVM_SET_GSI_ROUTING and KVM_CREATE_IRQCHIP
[ PATCH ] um : fix - up schedule_timeout () usage
ARM : OMAP : board - omap4panda : MUX configuration for sys_nirq2
[ SCSI ] Fix sense key MEDIUM ERROR processing and retry
staging : lustre : Replace max () with max_t ()
ARM : S3C64XX : DMA : Free node for non - circular queues
OMAP4 : Fix for CONTROL register Base
mac80211 : compete scan to cfg80211 if deferred scan fail to start
metag : export clear_page and copy_page
s390 / io : add ioport_map stubs
usb : isp1760 : check for null return from kzalloc
libceph : avoid NULL kref_put from NULL alloc_msg return
usb : gadget : dummy_hcd : clear usb_gadget region before registration
[ PATCH ] ieee1394 : fix cross_bound check for null ISO packets
viafb : fix IGA1 modesetting on VX900
[ libata ] ata_piix : add missing kfree ()
fs : Fix theoretical division by 0 in super_cache_scan ().
crypto : md5 - Set statesize
i2c - cpm : drop NO_IRQ
staging : android : ion : ion_dummy_driver . c Replace kzalloc () by kcalloc ()
[ SPARC64 ]: Fix pfn_pte () build failure .
xenbus : avoid zero returns from read ()
um : don ' t leak floating point state and segment registers on execve ()
iwlwifi : mvm : don ' t clear tbl -> win mistakenly
sfc : Clear I2C adapter structure in falcon_remove_nic ()
pNFS : Skip invalid stateids when doing a bulk destroy
[ media ] davinci : vpif_display : fix return type check for v4l2_subdev_call ()
fsl / fman : fix error handling
irda : Correctly clean up self -> ias_obj on irda_bind () failure .
drm / nouveau / bios / mxm : handle digital connector table 1 . 1
Btrfs : fix clone ioctl where range is adjacent to extent
ipc / shm . c : check for ulong overflows in shmat
KVM : x86 : preserve x2APIC LDR on INIT
drivers / rtc / rtc - da9055 . c : remove use of regmap_irq_get_virq ()
drm / nouveau / pci / gp104 : initial support
ceph : fscache : Update object store limit after file writing
cpufreq : prevent lockup on reading scaling_available_frequencies
staging : dgrp : fix potential NULL defereference issue
cpu / hotplug : Handle early registration gracefully
reparent_leader : check EXIT_DEAD instead of task_detached ()
USB : serial : fix potential heap buffer overflow
futex : Prevent attaching to kernel threads
ipv4 : Fix memory leak in exception case for splitting tries
wlcore : increase max roc duration to 30 seconds
drm / tilcdc : Remove unnecessary braces
cpufreq : powernow - k6 : Fix incorrect comparison with max_multipler
Staging : rc2860 : return - EFAULT on copy_to_user errors
staging : line6 : Fix unlocked snd_pcm_stop () call
ASoC : fsl_ssi : Fix samples being dropped at Playback startup
ARM : 7010 / 1 : mm : fix invalid loop for poison_init_mem
[ media ] drxk_hard : fix bad alignments
drbd : merge_bvec_fn : properly remap bvm -> bi_bdev
crypto : qat - update init_esram for C3xxx dev type
ARM : mx3 : mx31lilly : pass mc13783 SPI IRQ pin
pinctrl : tegra - xusb : fix an off by one test
hwmon : ( w83627ehf ) Force initial bank selection
Btrfs : don ' t wait around for new log writers on an SSD
x86 , efi : Check table header length in efi_bgrt_init ()
iommu / vt - d : Remove to_pci_dev () in intel_map_page ()
drm / i915 : Shut down PCH interrupts during irq_uninstall
staging : comedi : adv_pci1710 : update the MODULE_DESCRIPTION
[ PATCH ] Signedness issue in drivers / net / phy / phy_device . c
Btrfs : fix a scrub regression in case of write errors
Drivers : hv : balloon : Make the balloon driver not unloadable
PCI : Do not treat EPROBE_DEFER as device attach failure
Input : ads7846 - avoid pen up / down when reading hwmon
drm / nouveau : properly handle allocation failure in nouveau_sgdma_populate
irqdomain : Export irq_domain_free_irqs_common
[ SCSI ] lpfc : fix oops when parsing dodgy VPD
imx - drm : ipuv3 - plane : fix ipu_plane_dpms ()
xfrm_user : fix info leak in copy_to_user_policy ()
mei : abort waiting for notification on unsupported HW
x86 , mce : fix ifdef for 64bit thermal apic vector clear on shutdown
iwlwifi : range checking issue
[ media ] r820t : better report signal strength
usb : wusbcore : read actual_length bytes isoc in segments
[ GFS2 ] kernel panic mounting volume
[ SCSI ] libfc : handle RRQ exch timeout
iommu / amd : Free domain id when free a domain of struct dma_ops_domain
UIO : uio_pdrv : fix memory leak
drm / amdgpu : no cg for soc15 of SRIOV
scsi : ibmvscsis : Properly deregister target sessions
ASoC : DaVinci : pcm , constrain buffer size to multiple of period
KVM : PPC : e500 : Use kmalloc_array () in kvm_vcpu_ioctl_config_tlb ()
drm / radeon : fix endian bug in radeon_atom_get_mclk_range_table ()
ocfs2 : goto out_unlock if ocfs2_get_clusters_nocache () failed in ocfs2_fiemap ()
block : remove padding from io_context on 64bit builds
udf : Fix possible corruption when close races with write
lm8333 : Fix check ordering
usb : gadget : r8a66597 - udc : fix error return code in r8a66597_probe ()
[ PATCH ] utsns : fix ! CONFIG_UTS_NS behavior
spi : qup : Fix return value checking for pm_runtime_get_sync ()
ALSA : hda_intel : Add the Linux device ID for NVIDIA HDA controller
f2fs : avoid to trigger writepage during POR
[ media ] cx22700 : Fix potential buffer overflow
cfq : choose a new next_req when a request is dispatched
dmaengine : st_fdma : Fix the error return code in st_fdma_probe ()
[ media ] DaVinci - VPFE - Capture : Replace a memcpy () call by an assignment in vpfe_enum_input ()
KVM : PIT : free irq source id in handling error path
[ PATCH ] v9fs : replace strlen on newly allocated by __getname buffers to PATH_MAX
net : make ipv6 PKTINFO honour freebind
[ PATCH ] Fix listxattr () for generic security attributes
rpc : fix garbage in printk in svc_tcp_accept ()
[ NETFILTER ]: nfnetlink_log : fix possible NULL pointer dereference
Btrfs : skip adding an acl attribute if we don ' t have to
intel - iommu : fix off - by - one in pagetable freeing
hwmon : ( max31790 ) potential ERR_PTR dereference
staging : logger : hold mutex while removing reader
lib / genalloc . c : check result of devres_alloc ()
p54 : eliminate warning for uninitialized variable ' tim_len '
backlight : Fix backlight limiting on spitz / corgi devices
ath6kl : add missing of_node_put
drm / i915 / skl : Restore the DDI translation tables when enabling PW1
powerpc / eeh : Skip finding bus until after failure reporting
b43 : Fix warning at drivers / mmc / core / core . c : 237 in mmc_wait_for_cmd
drm / amdgpu : update pd while updating vm as well
iommu : smar : Fix return value check of create_irq ()
[ PATCH ] mempolicy . c GFP fix
drm / i915 : WARN if module opt sanitization goes out of order
[ 9P ]: Fix missing unlock before return in p9_mux_poll_start
cfg80211 : fix channel setting for wext
[ media ] stb0899 : don ' t go past DiSEqC msg buffer
ixgbe : balance free_irq calls with request_irq calls
usb : xhci : plat : Enable async suspend / resume
misc : apds9802als : Fix the logic checking timeout in als_wait_for_data_ready ()
i40e : Expose ' trust ' flag to userspace via ndo_get_vf_config .
crypto : ahash - ensure statesize is non - zero
virtio : console : Make read () return - ENODEV on hot - unplug
[ IPV6 ]: Reorg struct ifmcaddr6 to save some bytes
Input : gpio - keys - report error when disabling unsupported key
ext4 : avoid exposure of stale data in ext4_punch_hole ()
NFSD : Put export if prepare_creds () fail
[ PATCH ] drivers / message / i2o / pci . c : fix a NULL pointer dereference
[ NETFILTER ]: PPTP helper : fix sstate / cstate typo
xen : mask _PAGE_PCD from ptes
synclink : add mutex_unlock () on error path
i2c : Mark adapter devices with pm_runtime_no_callbacks
staging : omapdrm / omap_gem_dmabuf . c : fix memory leakage
isdn : remove duplicate NULL check
[ PATCH ] ioremap balanced with iounmap for drivers / serial / ioc4_serial . c
[ media ] drxd_hard : Add missing error code assignment before test
usb : musb : core : remove duplicate check on resource
[ media ] rc5 - decoder : BZ # 85721 : Fix RC5 - SZ decoding
staging : comedi : pcl816 : remove unnecessary ' dev -> irq ' check
[ PATCH ] corrupted cramfs filesystems cause kernel oops
[ CPUFREQ ] fix memory leak in cpufreq_add_dev
drm / nouveau / ltc : protect clearing of comptags with mutex
ieee1394 : sbp2 : fix probing of some DVD - ROM / RWs
proc : mem_release () should check mm != NULL
Drivers : hv : Fix a memory leak
inet : tcp : fix inetpeer_set_addr_v4 ()
drm / rockchip : return ERR_PTR instead of NULL
drivers / net / usb / usbnet . c : Use FIELD_SIZEOF macro in usbnet_init () function .
drm / i915 : take mode config lock around crtc disable at suspend
HID : i2c - hid : also call i2c_hid_free_buffers in i2c_hid_remove
staging : lustre : lustre : mdc : lproc_mdc . c : Fix for potential NULL pointer dereference
[ libata ] ata_piix : add HP compaq laptop to short cable list
drivers / platform / x86 / amilo - rfkill . c :: amilo_rfkill_probe () avoid NULL deref
cciss : add BUILD_BUG_ON () for catching bad CommandList_struct alignment
xfs : publish UUID in struct super_block
KVM : fix kvmclock regression due to missing clock update
perf probe : Fix regression of variable finder
[ SCSI ] fix two leaks in scsi_alloc_sdev failure paths
sh : fix a kfree address in clkdev code
[ PATCH ] pata_marvell : merge Mandriva patches
crypto : arm - replace memset by memzero_explicit
usb : r8a66597 : add locking to r8a66597_check_detect_child
usb : renesas_usbhs : gadget : fix NULL pointer dereference in ep_disable ()
[ media ] v4l : of : Drop acquired reference to node when getting next endpoint
tmiofb : missing NULL pointer checks
drm : mali - dp : Set the drm -> irq_enabled flag to match driver ' s state .
[ PATCH ] x86_64 : Fix swiotlb dma_alloc_coherent fallback
slub : When allocating a new slab also prep the first object
block : check partition alignment
eCryptfs : protect crypt_stat -> flags in ecryptfs_open ()
i915 : fix lock imbalance on error path ...
perf record : No build id option fails
Staging : Fix leak in drivers / staging / at76_usb . c
Btrfs : check if previous transaction aborted to avoid fs corruption
OMAPDSS : HDMI5 : disable interlace modes
drm / radeon : don ' t share PPLLs on DCE4 . 1
drm / i915 / vlv : disable PPGTT on early revs v3
xtensa : fix potential NULL - pointer dereference
[ PATCH ] pcmcia : permit single - character identifiers
[ PATCH ] uml : fix sizeof usage
dma : imx - sdma : Assign a default script number for ROM firmware cases
cfq - iosched : handle failure of cfq group allocation
[ PATCH ] x86 - 64 : unexport cpu_llc_id
staging : sm7xx : Fixup of the section mismatch
drm / i915 : take struct mutex around fb unref
ceph : drop extra open file reference in ceph_atomic_open ()
mfd : Check for twl4030 - madc NULL pointer
drm / radeon / kms : Fix oops after radeon_cs_parser_init () failure .
i2c : robotfuzz - osif : Constify osif_table
latencytop : fix kernel panic while reading latency proc file
video : vt8500lcdfb : Remove kfree call since devm_kzalloc () is used
vfio / spapr_tce : Check kzalloc () return when preregistering memory
[ SCSI ] zfcp : Fix error checking for ELS ADISC requests
i2c / ds1374 : Check workqueue creation status
Btrfs : align offsets for btrfs_ordered_update_i_size
tcp : fastopen : avoid negative sk_forward_alloc
fbcon : delay the start of the cursor timer until a con_switch ()
perf symbols : Fix missing GNU IFUNC symbols
staging : greybus : make cport_quiesce () method optional
mac80211 : mesh only parameter mppath maybe unused
jbd2 : Fix unreclaimed pages after truncate in data = journal mode
bcache : fix use - after - free in btree_gc_coalesce ()
[ ALSA ] sound / isa / ad1816a / ad1816a . c : check kmalloc () return value
[ SCSI ] aha152x : Fix oops on module removal
[ NETFILTER ]: H . 323 helper : fix possible NULL - ptr dereference
ASoC : si476x : Fix IO setup
ARM : 7857 / 1 : dma : imx - sdma : setup dma mask
batman - adv : Detect missing primaryif during tp_send as error
amd64_edac : fix ECC checking
iommu / ipmmu - vmsa : Restrict IOMMU Domain Geometry to 32 - bit address space
staging : comedi : adv_pci_dio : fix bug in ' detach '
drbd : Null pointer deref fix to the large " multi bio rewrite "
drm / i915 : Allow full PPGTT with param override
staging : fix go7007 - usb license
[ S390 ] cio : dont forget to set last slot to NULL in ccw_uevent ().
gpio : remember to finally free gpio_device
staging : dgap : fix checkpatch warning on sscanf usage
sctp : fix auth_hmacs field ' s length of struct sctp_cookie
mtd : spi - nor : improve macronix_quad_enable ()
[ S390 ] cio : allow setting boxed devices offline
scsi : handle command allocation failure in scsi_reset_provider
extcon : trivial : kfree missed from remove path
OMAPDSS : hdmi5 : Fix bit field for IEC958_AES2_CON_SOURCE
tipc : check return value of nlmsg_new
crypto : sahara - fix typo " Decidated " -> " Dedicated "
Input : polldev - immediately poll device upon opening
r8152 : replace strncpy with strlcpy
btrfs : rw_devices shouldn ' t be incremented for seed fs in btrfs_rm_dev_replace_srcdev ()
[ media ] v4l2 - ctrls . c : add NULL check
Staging : iio : Remove line over 80 characters
mlxsw : spectrum : Don ' t take multiple references on a FID
cpufreq : Return error if -> get () failed in cpufreq_update_policy ()
libertas : don ' t clear priv -> dnld_sent after sending sleep confirm
jfs : avoid undefined behavior from left - shifting by 32 bits
ceph : clear migrate seq when MDS restarts
xenbus : remove transaction holder from list before freeing
[ SCSI ] pm8001 : bit set pm8001_ha -> flags
tcp : fix tcp_cong_avoid_ai () credit accumulation bug with decreases in w
ACPI : thinkpad - acpi : do not arm fan watchdog if it would not work
clk : hisilicon : Use devm_kmalloc_array () in hisi_clk_alloc ()
iio : ak8975 : Add device name
bzip2 / lzma : fix decompress_inflate . c vs multi - block - with - embedded - filename
flex_array : fix the panic when calling flex_array_alloc () without __GFP_ZERO
spi : spi - ti - qspi : reinit of completion variable
[ ARM ] pxa / csb726 : register ac97 controller
drm / radeon : fix a bug in the SA code
[ PATCH ] USB : don ' t allocate dma pools for PIO HCDs
xfs : fix assertion failure at xfs_setattr_nonsize
usb : gadget : f_printer : actually limit the number of instances
spi : core : Fix check for dma_map_sg () failures
batman - adv : Count all non - success TX packets as dropped
staging : rts_pstor : Complete scanning_done variable
blkio : Set must_dispatch only if we decided to not dispatch the request
[ NETFILTER ]: nf_log : add netfilter gcc printf format checking
b43 : fix support for 14e4 : 4321 PCI dev with BCM4321 chipset
[ POWERPC ] spufs : check spu_acquire_runnable () return value
apm_power : check I . intval for zero value , we use it as the divisor
Staging : comedi : dt2801 . c : off by one issue
[ PATCH ] manage - jbd - its - own - slab fix
pinctrl : mediatek : add input - enable setting in gpio_request_enable
s390 / scm_blk : fix memleak in init function
scsi_dh : return SCSI_DH_NOTCONN in scsi_dh_activate ()
[ GFS2 ] Fix memory allocation in glock . c
sched : Try to deal with low capacity , fix update_sd_power_savings_stats ()
x86 : Update UV3 hub revision ID
Input : gamecon - fix off by one range check
staging / rts_pstor : remove braces {} in sd . c ( sd_sdr_tuning )
[ MIPS ] time : set clock before clockevent_delta2ns () in GT641xx .
gro : Fix bogus gso_size on the first fraglist entry
drm / i915 : Don ' t call sg_free_table () if sg_alloc_table () fails
rndis_wlan : add return value validation
drm : fix division - by - zero on dumb_create ()
KVM : fix fault page leak
qlcnic : Fix ordering of stats in stats buffer .
fs / exofs / ore_raid . c : replace count * size kzalloc by kcalloc
staging : comedi : pcl726 : update MODULE_DESCRIPTION
drm / tegra : Use lockless gem BO free callback
Btrfs : remove dead code
mmc : sdhci - esdhc - imx : silence a false curly braces warning
be2net : Bug fix to avoid soft lockup in loopback test .
iwlwifi : mvm : initialise ADD_STA before sending it to the firmware
[ PATCH ] device - mapper snapshot : bio_list fix
staging : vt6655 : check ieee80211_bss_conf bssid not NULL
ASoC : omap - mcbsp : fix snd_pcm_hw_rule_add arguments
eeepc - wmi : set the right key code for 0xe9
drm / nouveau : Fix detection of DDC - based LVDS on DCB15 boards .
[ SPARC64 ]: Use alloc_pci_dev () in PCI bus probes .
ASoC : Make struct snd_soc_card ' s dapm_widgets and dapm_routes const
ceph : add missing locking to protect i_snap_realm_item during split
Input : atmel_mxt_ts - add bootloader addresses for new chips
xfs : invalidate allocbt blocks moved to the free list
ravb : Add missing free_irq () call to ravb_close ()
ASoC : Free card DAPM context on snd_soc_instantiate_card () error path
drm / i915 : Fix up some stray to_i915 ( dev ) after a recent merge
bnx2x : Prevent " Unknown MF " print in SF mode
w1 : w1_process () is not freezable kthread
ARM : zynq : initialize slcr mapping earlier
[ NETFILTER ]: xt_pkttype : Add explicit check for IPv4
Btrfs : add fiemap ' s flag check
ceph : handle ceph_osdc_new_request failure in ceph_writepages_start
xen / balloon : Don ' t continue ballooning when BP_ECANCELED is encountered
[ PATCH ] hdaps : use ENODEV
mei : reset client connection state on timeout
ARM : OMAP2 : Prepare cpu detection for further improvements
spi : spidev : fix possible NULL dereference
drm / radeon / dpm : fix typo in setting uvd clock
ext4 : remove another test in ext4_alloc_file_blocks ()
[ PATCH ] mv643xx_eth : Update dev -> last_rx on packet receive
tmpfs : truncate prealloc blocks past i_size
igb : Bump version to 3 . 0 . 6
rt2x00 : Shortcut link state updates when not operating as STA
kvm / s390 : also set guest pages back to stable on kexec / kdump
staging / lustre / llite : Fix integer overflow in ll_fid2path
USB : isp1760 : Use an IS_ERR test rather than a NULL test
perf tools : Do not auto - remove Children column if -- fields given
[ media ] marvell - cam : Don ' t signal multiple frame completions in scatter / gather mode
hwmon : ( f75375s ) Disable setting DC fan control mode for F75373
ux500 : use _cansleep GPIO functions
usb : musb : blackfin : fix typo in new bfin_musb_vbus_status func
[ TCP ]: Clear stale pred_flags when snd_wnd changes
dmaengine : zxdma : Fix off - by - one for testing valid pchan request
[ S390 ] hwsampler : allow cpu hotplug
ARM : OMAP2 +: Fix dmtimer set source clock failure
btrfs : remove unnecessary list_del
drivers / net / cxgb3 / cxgb3_main . c : prevent reading uninitialized stack memory
mfd : twl4030 - power : Fix hang on reboot if sleep configuration was loaded earlier
macvtap : advance iov iterator when needed in macvtap_put_user ()
atl1c : return PCI_ERS_RESULT_DISCONNECT on permanent failure
lpfc : Fix internal loopback failure .
s2io : Don ' t receive packets when the napi budget == 0
drm / tilcdc : Take CRTC lock when calling tilcdc_crtc_disable ()
[ X25 ]: Restore skb -> dev setting in x25_type_trans ().
[ PATCH ] PCI : fix memory leak in MMCONFIG error path
mfd : Change tunnelcreek watchdog name on the lpc_sch subdevices array
staging : comedi : jr3_pci : check PCI BAR 0 size
uwb : Fix handling IEs with empty IE data in uwb_est_get_size ()
Staging : ramzswap : Set block size to PAGE_SIZE
dns_resolver : assure that dns_query () result is null - terminated
drm / i915 : CR clock gating is recommend to be set on PineView
misc : mic : Fix for double fetch security bug in VOP driver
drm / i915 / bxt : Use PHY0 GRC value for HW state verification
spi : spi - davinci : Fix variable type
evm : properly handle INTEGRITY_NOXATTRS EVM status
clk / axs10x : Clear init field in driver probe
acer - wmi : set the touchpad toggle key code to KEY_TOUCHPAD_TOGGLE
perf symbols : Fix random fd closing with no libelf
UBI : always re - read in case of read failures
[ CIFS ] add extra null attr check
rt2800 : fix wrong TX power compensation
[ media ] bt8xx : fix memory leak
logfs : maintain the ordering of meta - inode destruction
drm / amdgpu : update rev id for oland
ASoC : Check for errors when writing WM8731 reset register
drivers / perf : arm_pmu : Avoid leaking pmu -> irq_affinity on error
[ POWERPC ] EEH : Work with device endpoint , always
[ media ] DVB , DiB9000 : Fix leak in dib9000_attach ()
virt / kvm / assigned - dev . c : Set ' dev -> irq_source_id ' to '- 1 ' after free it
[ media ] drx - j : Fix detection of no signal
[ ALSA ] trident - Shut up compile warnings
libata : drop WARN from protocol error in ata_sff_qc_issue ()
USB : cyberjack : fix buggy integer overflow test
xfs : Use kmem_free () instead of free ()
mfd : Constify WM8994 regulator_init_data
Btrfs : don ' t make a file partly checksummed through file clone
drm / amd / powerplay : fix memory leak of tdp_table
f2fs : allocate new segment aligned with sections
leak in do_ubd_request
usbnet : smsc95xx : don ' t recover device if suspend fails in system sleep
thermal : imx : Fix possible NULL dereference .
ARM : shmobile : mackerel : remove OCR masks , where regulators are used
[ SCSI ] isci : fix breakage caused by > 16byte CDB patch
KVM : PPC : Book3S HV : Don ' t try to signal cpu - 1
arch / powerpc / sysdev / fsl_rmu . c : introduce missing kfree
ASoC : pcm3168a : Call clk_set_rate in pcm3168a_set_dai_sysclk
ALSA : asihpi : Fix unlocked snd_pcm_stop () call
exit : proc : don ' t try to flush / proc / tgid / task / tgid
gpio : use kzalloc to allocate gpio_device
ALSA : lx646es : Fix possible uninitialized variable reference
lkdtm : do not leak free page on kmalloc failure
ACPICA : fix mutex names in debug code .
PCI : of : Add 64 - bit address recognition without LPAE support
proc : task_state : read cred -> group_info outside of task_lock ()
crypto : amcc - check return value of sg_nents_for_len
Btrfs : fix incorrect space accounting after failure to insert inline extent
rapidio / tsi721 : fix incorrect detection of address translation condition
x86 / acpi : Prevent LAPIC id 0xff from being accounted
staging : et131x : stop read when hit max delay in et131x_phy_mii_read
usb : fusbh200 - hcd : fix error handling in fusbh200_hcd_fusbh200_probe ()
drm / i915 : enable HDMI mode on VLV when an HDMI sink is detected
cxgb4 : fix memory leak of qe on error exit path
crypto : talitos - avoid memleak in talitos_alg_alloc ()
kobject : free memory if netlink_kernel_create () fails
[ ALSA ] gusextreme : set codec_flag
x86 / apic : Use x2apic physical mode based on FADT setting
drivers / staging / lustre : Fix set - but - unused whinge .
xfs : silence uninitialised f . file warning .
mac80211 : fix ieee80211_sta_process_chanswitch for 5 / 10 MHz channels
staging : sep : add missing misc_deregister () on error in sep_register_driver_with_fs ()
mfd : palmas : Initialise client -> of_node for dummy created client
KVM : ARM : VGIC : Optimize the vGIC vgic_update_irq_pending function .
drivers / net / hamradio : Integer overflow in hdlcdrv_ioctl ()
[ ALSA ] Return error if no user TLV is defined
[ ATM ] firestream : Fix uninitialized var warning .
inetpeer : fix token initialization
[ media ] lirc_igorplugusb : Fix a compilation waring
tridentfb : fix hang on Blade3D with CONFIG_CC_OPTIMIZE_FOR_SIZE
perf tools : Change perf_guest default back to false
KVM : MMU : make for_each_shadow_entry aware of largepages
squashfs : add missing buffer free
mfd : Check t7l66 platform_data pointer
dmaengine : omap - dma : Add more debug information when freeing channel
[ PATCH ] v9fs : fix fd_close
mm , vmstat : Remove spurious WARN () during zoneinfo print
[ SCSI ] mpt2sas : add missing allocation .
ARM : Fix build error for SMP = n builds
drm / vmwgfx : Don ' t flush fb if we ' re in the suspended state .
sh - pfc : r8a7790 : Fix definition of IPSR5
[ ARM ] pxa : add missing irq events for pxa168
sysctl : require CAP_SYS_RAWIO to set mmap_min_addr
[ media ] dvb_demux : fix deadlock in dmx_section_feed_release_filter ()
ext4 : Fix ext4_free_blocks () w / o a journal when files have indirect blocks
drm / i915 : Convert wait_for ( I915_READ ( reg )) to intel_wait_for_register ()
[ PATCH ] inotify : fix idr_get_new_above usage
ALSA : ALC283 codec - Avoid pop noise on headphones during suspend / resume
iwlwifi : fix PS disable status race
[ PATCH ] libata : fix missing classes [] initialization in ata_bus_probe ()
ASoC : rsnd : rsnd_dai_connect () returns error if it connect to existing mod
usbfs : Fix oops related to user namespace conversion .
blk - throttle : check stats_cpu before reading it from sysfs
Btrfs : don ' t loop in allocator for space cache
cpufreq : brcmstb - avs - cpufreq : remove unnecessary platform_set_drvdata ()
ipv6 : Fix mem leak in rt6i_pcpu
fealnx : Write outside array bounds
mac802154 : llsec : add forgotten list_del_rcu in key removal
[ CPUFREQ ] powernow - k8 : get drv data for correct CPU
trusted - keys : check for NULL before using it
ASoC : intel : log an error on double free
selinux : check ss_initialized before revalidating an inode label
firewire : nosy : fix IRQ handler for card ejection
staging : bcm2835 - audio : Fixed initialization of global to " false "
unix / dgram : peek beyond 0 - sized skbs
greybus : uart fix missing negation on DTR setting
mlx5_core : Fix out arg size in access_register command
[ PATCH ] VFS : memory leak in do_kern_mount ()
KVM : nVMX : Remove redundant get_vmcs12 from nested_vmx_exit_handled_msr
drm / radeon : Fail fb creation from imported dma - bufs .
ipv6 - multicast : Fix memory leak in input path .
[ PATCH ] imsttfb : Fix resource leak
perf hists browser : Add NULL pointer check to prevent crash
[ PATCH ] m32r : __cmpxchg_u32 fix
ARM : SMP : remove IRQ - disabling for smp_cross_call ()
drm / amdgpu / si : load the proper firmware on 0x87 oland boards
igb : add i211 to i210 PHY workaround
b43 : Force - wake queues on init
[ SCSI ] libfcoe : don ' t send ELS in FIP mode if no FCF selected
i82875p_edac : fix overflow device resource setup
[ PATCH ] Uninitialized variable in drivers / net / wan / syncppp . c
e1000 : fix WARN_ON with mac - vlan
drivers : usb : Mark function as static in usbsevseg . c
[ PATCH ] USB : get USB suspend to work again
wl12xx : fix a memory leak of probereq template upon recovery
[ media ] marvell - cam : power down mmp camera on registration failure
drm : i915 : Explicitly apply PWM config extracted from pwm_args
ceph : fix leak of mon authorizer
spi : sh - msiof : Fix warnings due to improper casts
tmpfs : fix highmem swapoff crash regression
ASoC : Return correct codec clock in s3c64xx - i2s
sysctl : Add neg_one as a standard constraint
ath9k : deny new interface addtion on IBSS mode
x86 : Re - get cfg_new in case reuse / move irq_desc
NFS : Return directly if encode_sessionid fail
iio : light : ltr501 bug in parameter sanity check .
kernel / kmod . c : check for NULL in call_usermodehelper_exec ()
btrfs : reada : move reada_extent_put to place after __readahead_hook ()
staging : wilc1000 : remove spaces around '->'
spi - imx : initialize complete config struct
x86 , amd - ucode : Check UCODE_MAGIC before loading the container file
sdhci : make isr tolerant of read errors
misc : mei : set IRQF_ONESHOT for msi request_threaded_irq
iw_cxgb4 : initialize ibdev . iwcm -> ifname for port mapping
edac : mv64x60 fix get_property
i40iw : Add NULL check for ibqp event handler
x86 / mce / AMD : Make the init code more robust
usb : dwc3 : gadget : return error if command sent to DGCMD register fails
bpf , mips : fix off - by - one in ctx offset allocation
igb : Change memcpy to struct assignment
macvlan : make operstate and carrier more accurate
i2c : mux : reg : Provide of_match_table
[ PATCH ] Fix warning with b44 . c on 64bit boxes
p54usb : fix leaks at failure path in p54u_probe ()
drivers / w1 / masters / omap_hdq . c : fix missing mutex unlock
drm / exynos / fbdev : set smem_len for fbdev
f2fs : fix wrong condition for unlikely
ALSA : snd - powermac : timeout reaches - 1
af_packet : remove unnecessary BUG_ON () in tpacket_destruct_skb
ARM : EXYNOS : fix GIC using for EXYNOS5440
Drivers : hv : vss : run only on supported host versions
tracing / filters : free filter_string in destroy_preds ()
i2c : i2c - bcm - kona : Fix module build
drm / amd / amdgpu / amdgpu_ttm : Handle return NULL error from ioremap_nocache
[ ALSA ] soc - Clean up tabs
libceph : avoid NULL kref_put when osd reset races with alloc_msg
dmaengine : coh901318 : Only calculate residue if txstate exists .
ssb : Use pci_is_pcie ()
[ PATCH ] fix PROMISC / bridging in TLAN driver
vmwgfx : information leak in vmw_execbuf_copy_fence_user ()
xfrm : Ignore socket policies when rebuilding hash tables
USB : add a private - data pointer to struct usb_tt
[ PPPOE ]: Missing result check in __pppoe_xmit ().
KVM : Grab pic lock in kvm_pic_clear_isr_ack
[ PATCH ] x86 : initialise tss -> io_bitmap_owner to something
davinci : DM365 : Update NAND driver platform data
arch / x86 / kernel / kdebugfs . c : introduce missing kfree
drm / i915 : switch off backlight for backlight class 0 brightness
NetLabel : the CIPSOv4 passthrough mapping does not pass categories correctly
x86 / boot : Fix minor fd leakage in tools / relocs . c
staging : comedi : das16 : use comedi_handle_events ()
sound : oxygen : zero - initialize model data
[ media ] uvcvideo : Fix race of open and suspend in error case
bnx2x : set_one_mac_e1x uses raw ' s state as input
USB : yurex : fix spelling mistake in comment
hfsplus : fix possible deadlock when handling corrupted extents
rxrpc : Fix oops on incoming call to serviceless endpoint
appletalk : fix atalk_getname () leak
tracing : Reset parser -> buffer to allow multiple " puts "
ASoC : soc - core : Fix codec -> name memory leak
staging : octeon - usb : Cocci spatch " noderef "
Staging : comedi : kcomedilib : replace deprecated simple_strtoul () with kstrtouint ()
[ NETFILTER ]: recent match : missing refcnt initialization
Btrfs : remove btrfs_try_spin_lock
slab : fix typo in allocation failure handling
drm / ttm : fix stupid parameter inversion in the pipeline code
Input : tegra - kbc - correct call to input_free_device
pinctrl : sunxi : Fix potential null pointer dereference
usb : early : fixed coding style issue related to : operator
iio : adc : ad799x : remove space before comma
ARM : shmobile : armadillo800eva : set clock rates before timer init
[ TCP ]: Fix oops caused by __tcp_put_md5sig_pool ()
Staging : rtl8192e : Fix potential NULL pointer dereference
microblaze : Ptrace notifying from signal code
capabilities : initialize has_cap
[ ARM ] 4056 / 1 : iop13xx : fix resource . end off - by - one in flash setup
usb : udc : allow adding and removing the same gadget device
i40e : Fix memory leak at failure path in i40e_dbg_command_write ()
[ PATCH ] TPM : fix failure path leak
staging : most : add missing channel initialization
ceph : skip mds sync on forced unmount
remove header_ops bug in qeth driver
IPv6 : use ipv6_addr_copy () in ip6_route_redirect ()
bnx2x : fix possible panic under memory stress
staging : comedi : disallow COMEDI_DEVCONFIG on non - board minors
firewire : fw - cdev : reorder wakeup vs . spinlock
iio : ina2xx : give the capture kthread a more useful name string .
hwmon : ( emc1403 ) Remove unnecessary hwmon_device_unregister
[ PATCH ] x86 - 64 : add memcpy / memset prototypes
ath9k : Do not use local_bh_disable in ampdu_action
virtio_net : enable big packets for large MTU values
[ PATCH ] i4l : memory leak fix for sc_ioctl ().
samples / bpf : Reset global variables
usbnet : allow mini - drivers to consume L2 headers
linux / kernel . h : define SYMBOL_PREFIX
[ ARM ] 5402 / 1 : fix a case of wrap - around in sanity_check_meminfo ()
slub : Discard slab page when node partial > minimum partial number
fs : make inode_to_bdi () handle NULL inode
slab : fix alien cache handling
block / partitions / aix . c : replace count * size kzalloc by kcalloc
iwlwifi : mvm : disable uAPSD due to bugs in the firmware
iommu / exynos : Add / remove callbacks should fail if no iommu is available
[ PATCH ] Fix typo in x86_64 __build_write_lock_const assembly
mac80211 : fix txq queue related crashes
kernel / sys . c : fix missing rcu protection for sys_getpriority ()
[ PATCH ] gss_spkm3 : fix error handling in module init
atm : deal with setting entry before mkip was called
drm / atomic : Wait for vblank whenever a plane is added to state .
mm : vma_adjust : remove superfluous check for next not NULL
pty : Fix packet mode setting race
qed : Fix error return code in qed_resc_alloc ()
vhost : actually track log eventfd file
[ TCP ] FRTO : Response should reset also snd_cwnd_cnt
staging : comedi : prevent auto - unconfig of manually configured devices
[ PATCH ] i386 : fix flat mode numa on a real numa system
dlm : fix possible use - after - free
KVM : fix cpuid E2BIG handling for extended request types
sfc : Use kzalloc () to ensure struct efx_spi_device is fully initialised
mac80211 : Don ' t buffer non - bufferable MMPDUs
usb : dwc3 : ep0 : move to CONFIGURED also on delayed status
cpufreq : intel_pstate : Fix setting max_perf_pct in performance policy
orangefs : avoid freeing a slot twice in wait_for_direct_io ()
KVM : VMX : Only reload guest msrs if they are already loaded
drivers / usb / misc / auerswald : fix status check , remove redundant check
dsa : mv88e6xxx : Fix error handling in mv88e6xxx_set_port_state
tty / vt : Fix the memory leak in visual_init
AUDIT : Fix user pointer deref thinko in sys_socketcall ().
ide : add missing rq . ref_count initialization to ide_diag_taskfile ()
uml : drivers / net_user . c memory leak fix
ath9k : enable ext channel pulse detection
i40e : Fix a port VLAN configuration bug
ARM : S3C64XX : Remove unused CPU devices for Cragganmore
KVM : x86 : Improve thread safety in pit
perf trace : Fix detection of script extension
ALSA : hdspm - Abort on unknown firmware revision
staging : ks7010 : change length type to unsigned
[ PATCH ] NPTL signal delivery deadlock fix
Fix a referral error Oops
x86 : fix MP_processor_info section mismatch warning
IPoIB : Clear ipoib_neigh . dgid in ipoib_neigh_alloc ()
futexes : Fix infinite loop in get_futex_key () on huge page
staging : rts5208 : Unnecessary parentheses cleanup
drm / i915 / skl : Don ' t skip mst encoders in skl_ddi_pll_select ()
bnx2x : Add known PHY type check
ath9k : Check virtual wiphy state on tx ()
ASoC : imx - es8328 : Register jacks at the card level
tty : fix vt_compat_ioctl
[ ARM ] Use kcalloc to allocate counter_config array rather than kmalloc
USB : belkin_sa : avoid divide by zero error
drm / nouveau : v3 . 0 pll limits tables have type <-> register mapping too
fscrypt : use EEXIST when file already uses different policy
brcmfmac : check return from kzalloc in brcmf_fweh_process_event
sfc : Really allow RX checksum offload to be disabled
[ POWERPC ] spufs : fix another off - by - one bug in spufs_mbox_read
perf tools : Fix map_groups__clone to put cloned map
device - dax : check devm_nsio_enable () return value
be2net : Check for POST state in suspend - resume sequence
inetpeer : initialize -> redirect_genid in inet_getpeer ()
perf / hwbp : Fix cleanup in case of kzalloc failure
ASoC : Samsung : Add I2S S / W RST in startup function
ARM : imx : ehci : use void __iomem * to hold i / o addresses
USB : serial : digi_acceleport : fix OOB - event processing
drm / i915 / skl : Ensure pipes with changed wms get added to the state
USB : g_mass_storage : min (...) warning fixed
[ media ] s5p - jpeg : remove stray call to readl
[ SCSI ] ibmvfc : Fix invalid error response handling
drbd : fix spurious protocol error
drm / radeon : add backlight quirk for hybrid mac
ASoC : rt298 : fix null deref on acpi driver data
[ media ] cx23885 - dvb : check if dvb_attach () succeded
ceph : fix ack counter reset on connection reset
mac80211 : fix check for buffered powersave frames with txq
tty : xuartps : Fix " may be used uninitialized " build warning
[ IPV6 ] ROUTE : Allow searching subtree only .
virtio : console : We support only one device at a time
w1 : fix for loop in w1_f29_remove_slave ()
cifs : always do is_path_accessible check in cifs_mount
Btrfs : disable snapshot aware defrag for now
iommu / vt - d : Check the return value of iommu_device_create ()
x86 / amd - iommu : Don ' t detach device from pt domain on driver unbind
inkern : iio_device_put after incorrect return / goto
i40evf : do not re - arm watchdog after remove
uas : Fix bounds check in uas_find_endpoints
KVM : s390 : kvm / sigp . c : fix memory leakage
drm / i915 : Prevent use - after - free of inherited framebuffer
drm / i915 : Don ' t access snooped pages through the GTT ( even for error capture )
[ SCSI ] advansys : Fix bug in AdvLoadMicrocode
cciss : remove unnecessary casts
tcm_vhost : Initialize vq -> last_used_idx when set endpoint
PCI : designware : Reject MSI - X IRQs
ARM : pxa : Fix Vpac270 gpio_power for MMC
staging : vt6656 : [ BUG ] out of bound array reference in RFbSetPower .
ALSA : hda - Fix a copmile warning
cdc - acm : fix crash if flushed with nothing buffered
USB : serial : digi_acceleport : fix OOB - event processing
drm / nouveau : recognise higher link rate for available dp bw calculations
econet : 4 byte infoleak to the network
btrfs : scrub : silence an uninitialized variable warning
drm / i915 / dp : Ratelimit DP aux timeout messages
ARM : 7772 / 1 : Fix missing flush_kernel_dcache_page () for noMMU
sysfs : sysfs_write_file () writes zero terminated data
sctp : kzalloc () error handling on deleting last address
staging : rtl8712 : Replace kmalloc with kmalloc_array
ALSA : line6 : Give up on the lock while URBs are released .
drm / rockchip : Shutdown all crtcs when unbinding drm
mtd : mtd - abi : add a helper to detect the nand type
tipc : fix variable dereference before NULL check
qlcnic : Initialize dcbnl_ops before register_netdev
staging : android : sync : Fix memory corruption in sync_timeline_signal ().
ALSA : asihpi : incorrect range check
orinoco : fix TKIP countermeasure behaviour
cfg80211 : fix potential connection work crash
dma : timb_dma : Fix compiler warning
mmc : sd : warn if card stays busy during init
carl9170 : fix typo in PS code
drm / i915 : Skip stolen region initialisation if none is reserved
drm / nouveau / bios / gp104 : initial support
Staging : wlan - ng : wiphy_free () is not called in case wiphy_register () fails
USB : Remove unnecessary casts of void ptr returning alloc function return values
drm / amdgpu : add zero timeout check in amdgpu_fence_wait_seq_timeout
tcm_fc : Fix crash seen with aborts and large reads
vfs : abort dedupe loop if fatal signals are pending
drm / amdgpu : bypass RLC init for SRIOV
pti : PTI semantics fix in pti_tty_cleanup .
clk : tegra : Register the proper number of resets
virtio : console : fix race in port_fops_open () and port unplug
rbd : fix a memory leak in rbd_get_client ()
lsm : Use a compressed IPv6 string format in audit events
UBI : bugfix : dont oops with NULL module parameter
[ IA64 ] SN2 : security hole in sn2_ptc_proc_write
x86 : put trigger in to detect mismatched apic versions
sysfs : Make poll behaviour consistent
[ media ] cx231xx : add a missing break statement
sched : terminate newidle balancing once at least one task has moved over
ASoC : Fix incorrect kfree in wm8731_probe error path
[ ALSA ] hdsp - Fix zero division
spi : davinci : do not use DMA for vmalloc ' ed buffers
kvm : irqchip : fix memory leak
[ media ] stb0899 : sign of CRL_FREQ doesn ' t depend on inversion
ASoC : ad1836 : fix a memory leak if another ad1836 is registered
[ MTD ] Initialise s_flags in get_sb_mtd_aux ()
mm : fail prefaulting if page table allocation fails
crypto : caam - Fix first parameter to caam_init_rng
ALSA : timer : Fix leak in SNDRV_TIMER_IOCTL_PARAMS
trace / kprobes : Add back warning about offset in return probes
IIO : Documentation : generic_buffer example : Avoid NULL pointer dereference
ARM : at91 : fix nommu build regression
qla2xxx : fix busy wait regression
dmaengine : edma : Correct the handling of src / dst_maxburst == 0
drivers : misc : pass miscdevice pointer via file private data
ocfs2 : send SIGXFSZ if new filesize exceeds limit - v2
nvmet : Use direct IO for writes
ASoC : s6000 : Use WARN_ON () instead of BUG_ON ()
rxrpc : rxrpc_kernel_send_data () needs to handle failed call better
block : ioctl : fix information leak to userland
staging : unisys : visornic : remove driver version from visornic_main . c
perf tools : Fix build breakage on arm64 targets
Staging : bcm : info leak in ioctl
clk : at91 : Fix memory leak in of_at91_clk_master_setup ()
ARM : 5623 / 1 : Treo680 : ir shutdown typo fix
dma : tegra20 - apbdma : err message correction
ocfs2 : avoid a pointless delay in o2cb_cluster_check ()
r8169 : fix a bug in rtl8169_init_phy ()
cdc_ncm : return - ENOMEM if kzalloc fails
[ IA64 ] missing exports hwsw_sync_ ...
kernel / irq / irqdomain . c : before use ' irq_data ', need check it whether valid .
cgroup : set ' start ' with the right value in cgroup_path .
drm / nouveau / bus / gp104 : initial support
pty : fix data loss when stopped (^ S /^ Q )
dm cache : do not wake_worker () in free_migration ()
xhci : Warn when hosts don ' t halt .
[ PATCH ] Bluetooth : fix potential NULL ptr deref in dtl1_cs . c :: dtl1_hci_send_frame ()
ipv6 : exit early in addrconf_notify () if IPv6 is disabled
tracing : instance_rmdir () leaks ftrace_event_file -> filter
tcp_output : fix sparse warning for tcp_wfree
gpiolib : Fix NULL pointer deference
[ GFS2 ] Plug an unlikely leak
rtl8150 : rtl8150_disconnect (...) does not need tasklet_disable (...)
[ PATCH ] v9fs : return the correct error when interrupted by signal
perf symbols : Properly align symbol_conf . priv_size
[ IPSEC ] Set byid for km_event in xfrm_get_policy
[ media ] media : davinci : vpif_display : drop unused member fbuffers
Btrfs : don ' t leak reloc root nodes on error
caif - hsi : Add missing return in error path
drm : Check if the allocation has succeeded before dereferencing newmode
xfs : extra semi - colon breaks a condition
tracing : Fix off - by - one in trace_get_user ()
[ media ] s5h1420 : fix a buffer overflow when checking userspace params
mtd : nand : tmio_nand : show parent device in sysfs
sky2 : only enable Vaux if capable of wakeup
[ PATCH ] i2c : i2c - ibm_iic add I2C_CLASS_HWMON
tty : fix race in tty_fasync
dnet : Support the get_ts_info ethtool method .
mac80211 : remove a bit of dead mesh code
brcmfmac : rework pointer trickery in brcmf_proto_bcdc_query_dcmd ()
Input : regulator - haptic - fix module autoload for OF platform driver
ipr : Driver version 2 . 6 . 2
mfd : sta2x11 - mfd : Only add sta2x11_mfd if it hasn ' t already been added
thinkpad_acpi : Fix a memory leak during module exit
[ AF_KEY ]: Fix bug in spdadd
drm / i915 : Make aliasing a 2nd class VM
md : Fix raid1 -> raid0 takeover
HSI : nokia - modem : fix error handling of irq_of_parse_and_map
tracing : Don ' t make assumptions about length of string on task rename
i40e : add check for null VSI
ixgbe : in shutdown , do netif_running () under rtnl_lock
PCI : keystone - dw : Update PCI config space remap function
tools lib traceevent : Free filter tokens in process_filter ()
Btrfs : init inode ordered_data_close flag properly
mmc : sdhci - bcm2835 : Clean up platform allocations if sdhci init fails .
blk - mq : export blk_mq_free_request ()
[ SCSI ] ibmvfc : Fix DMA mapping leak on memory allocation failure
staging : lustre : osc : osc_extent_wait () shouldn ' t be interruptible
drm / radeon : fix sad_count check for dce3
mtg : docg3 : potential divide by zero in doc_write_oob ()
[ media ] dvb - usb - gp8psk : Fix tuner timeout ( against git )
brcm80211 : Don ' t leak ' vbuffer ' in brcmf_sdbrcm_write_vars ()
spidernet : node - aware skbuff allocation
tracing / function - graph : Use correct string size for snprintf
arm64 : kernel : do not mark reserved memory regions as IORESOURCE_BUSY
OMAP : DSS2 : Check if display supports update mode changes
[ PATCH ] Fix sys_migrate_pages : Move all pages when invoked from root
jme : Fix VLAN memory leak
macvtap : fix uninitialized return value macvtap_ioctl_set_queue ()
regmap : Silence warning on invalid zero length read
leds : add LED_ON brightness as boolean value
workqueue : Add quiescent state between work items
rfs : call sock_rps_record_flow () in tcp_splice_read ()
[ PATCH ] md : Avoid oops when attempting to fix read errors on raid10
btrfs : sysfs label interface should check for read only FS
staging : greybus : arche - platform : compress return logic into one line
spi : spi - fsl - dspi : Fix incorrect freeing of DMA allocated buffers
ALSA : hda - Update automute / automic upon jack retasking
Btrfs : Fix test_range_bit for whole file extents
fsl_pq_mdio : Revive Gianfar TBI PHY support
tty : serial : Use GFP_ATOMIC instead of GFP_KERNEL in serial8250_em485_init ()
[ PATCH ] x86 - 64 : remove prototype of free_bootmem_generic ()
staging : usbip : Fix deadlock
brcmfmac : fix possible memory leak
gpu : host1x : Check for valid host1x pointer
drm / i915 : Fix NULL pointer deference when out of PLLs in IVB
drm : fixes when i2c encoder slave mode_fixup is null .
ocfs2 : unmap_mapping_range () in ocfs2_truncate ()
staging : rts5208 : Spacing
ARM : ensure vm_struct has its phys_addr member filled in
md : check for memory allocation failure in faulty personality
iwlwifi : mvm : Avoid signal based decisions if ave beacon RSSI is 0
[ SCSI ] qla4xxx : correct return status in function qla4xxx_fw_ready
hwmon : ( coretemp ) fix oops on cpu unplug
sysfs : Remove double free sysfs_get_sb
coredump : make coredump_wait wait for mmap_sem for write killable
staging : usbip : userspace : libsrc : ( foo *) should be ( foo *)
ALSA : hdsp - info leak in snd_hdsp_hwdep_ioctl ()
block : Enable sysfs nomerge control for I / O requests in the plug list
powerpc / powernv / ioda2 : Gracefully fail if too many TCE levels requested
agp / intel : Use a non - reserved value for the cache field of the PTEs .
ibm_newemac : Fix dangerous struct assumption
tty_audit : fix checkpatch complaint
drm / exynos : fimd : fix broken dp_clock control
iio : adc : mcp320x : Set struct spi_driver . of_match_table
xen - blkfront : remove IRQF_SAMPLE_RANDOM which is now a no - op
arm : mxs : add irq_chip - name for GPIO IRQs
sched : Don ' t set sd -> child to NULL when it is already NULL
mfd : Fix possible s5m null pointer dereference
staging : zcache : fix " zcache =" kernel parameter
mwl8k : prevent freeing free IRQ if ieee80211_register_hw () fails
[ media ] s5p - g2d : Remove casting the return value which is a void pointer
ASoC : Reset WM8962 with default ID value
tcp : zero ca_priv area when switching cc algorithms
[ media ] xc5000 : use after free in release ()
i40evf : don ' t blow away MAC address
greybus : uart : Update line coding settings only when needed
igb : use mac loopback for i354 backplane
slob : fix memory corruption
[ PATCH ] s390 : overflow in sched_clock
p54 : use ether_addr_equal_64bits
drivers / net / wireless / p54 / txrx . c Fix off by one error
[ PATCH ] invalidate_inode_pages2_range () debug
x86 / microcode / intel : Guard against stack overflow in the loader
ACPI : check acpi_disabled in acpi_table_parse () and acpi_table_parse_entries ()
igb : fix stats for i210 rx_fifo_errors
HID : extend autodetect to handle I2C sensors as well
r8169 : correct phy parameters for the 8110SC
HID : hid - magicmouse : Add pointer and buttonpad properties for Magic Trackpad
ASoC : nuc900 - ac97 : fix a memory leak
m68k : Export empty_zero_page for ZERO_PAGE usage in modules .
cgroup : remove redundant code in cgroup_rmdir ()
usb : dwc3 : omap : use the macro we already have
xfrm : Fix truncation length of authentication algorithms installed via PF_KEY
spi : davinci : remove unnecessary completion variable initialization
perf probe : Fix the missed parameter initialization
mwl8k : declare MODULE_FIRMWARE
OMAPDSS : add missing export for omap_dss_get_output ()
selinux : fix setting of security labels on NFS
ASoC : simple - card : Add a NULL pointer check in asoc_simple_card_dai_link_of
virtio : delete vq from list
unicore32 / irq : Prepare puv3_gpio_handler for irq argument removal
ALSA : maestro3 : Clear interrupts before enabling them
trusted keys : Fix a memory leak in trusted_update ().
mm , hugetlb_cgroup : align hugetlb cgroup limit to hugepage size
[ IRDA ]: Fix memory leak in irttp_init ()
ipv4 : netfilter : ip_tables : fix information leak to userland
staging : unisys : Add checks for creation
wl1271 : Fix mac80211 RTS threshold requests during WL1271_STATE_OFF
i40evf : don ' t stop watchdog if it hasn ' t started
dm cache : fix error return code in cache_create
ASoC : dpcm : Add checks of playback / capture before dpcm_get_be
ath9k / htc_drv_main : off by one error
[ PATCH ] USB : Fix typo in ohci - ppc - soc . c : usb_hcd_put => usb_put_hcd
Btrfs : fix possible memory leak in btrfs_create_tree ()
HID : fix error condition propagation in hid - sony driver
[ media ] Staging : media : platform : davinci : - Fix for memory leak
perf tools : Make vmlinux short name more like kallsyms short name
fuse : Initialize total_len in fuse_retrieve ()
powerpc / pci : export pcibios_free_controller ()
[ media ] tveeprom : add support for Xceive XC5000C tuner
devm_memremap : Fix error value when memremap failed
sh_eth : Check for DMA mapping errors on transmit
ath9k_hw : fix an off - by - one error in the PDADC boundaries calculation
ALSA : hda / realtek - Avoid invalid COEFs for ALC271X
stmmac : fix oversized frame reception
mwifiex : update amsdu tx packet time stamp
tcp : Report mss_clamp with TCP_MAXSEG option in repair mode
[ CPUFREQ ] Longhaul - Fix power state test to do something more useful
[ media ] lirc : cannot read from tx - only device
pm8001 : Fix to remove null pointer checks that could never happen
ASoC : wm8750 : Replace direct snd_soc_codec dapm field access
Btrfs : stop leaking btrfs_bios on readahead
staging : frontier : use after free in disconnect ()
[ PATCH ] md : bio leak fix
sis - agp : Remove SIS 760 , handled by amd64 - agp
i2c : Align i2c_device_id
iwlwifi : fix PCI - E transport " inta " race
firewire : core : fix an information leak
[ MTD ] [ OneNAND ] fix call to onenand_verify when writing subpages
perf kvm record / report : ' unprocessable sample ' error while recording / reporting guest data
cpuset : use trialcs -> mems_allowed as a temp variable
staging : lustre : llite : fixup return value ll_direct_IO_26
cifs : potential missing check for posix_lock_file_wait
net / 9p / protocol . c : Fix a memory leak
mtd : socrates_nand : Remove redundant dev_set_drvdata
crypto : talitos - Fix timing leak in ESP ICV verification
drm / radeon / si_dpm : Limit clocks on HD86xx part
rtlwifi : rtl8192cu : Fix unitialized struct
audit : log module name on delete_module
staging : tidspbridge : fix potential array out of bounds write
xfs : fix memory leak in xlog_recover_add_to_trans
iwlagn : use vif -> type to check station
rxrpc : Validate the net address given to rxrpc_kernel_begin_call ()
Staging : batman - adv : Remove NULL pointer check
cpuidle , menu : use interactivity_req to disable polling
OMAP3 : PM : fix lockdep warning caused by omap3_pm_init
[ NETFILTER ]: nf_conntrack_sip : clear address in parse_addr ()
xfs : prevent leaking uninitialized stack memory in FSGEOMETRY_V1
xHCI : add XHCI_RESET_ON_RESUME quirk for VIA xHCI host
ceph : fix memory leak due to possible dentry init race
[ media ] r820t : Fix IF scale
drm / radeon / kms : Don ' t kzalloc memory which is immediately overwritten .
drm / i915 : Add lock on drm_helper_resume_force_mode
virt : vbox : fix memory leak in hgcm_call_preprocess_linaddr
drm : when queuing an event with NEXTONMISS , return queued sequence to userspace
mwifiex : Fix possible buffer overflows in mwifiex_ret_wmm_get_status ()
parisc : fix wrong page aligned size calculation in ioremapping code
ASoC : Intel : Skylake : Don ' t unload module when in use
firestream : fix spelling mistake : " reseverd " -> " reserved "
ASoC : ssm4567 : Reset device before regcache_sync ()
KVM : use heuristic for fast VCPU lookup by id
scsi : megaraid_sas : return error when create DMA pool failed
ext4 : disable COLLAPSE_RANGE for bigalloc
wlcore : fix unsafe dereference of the wlvif
perf / ftrace : Fix paranoid level for enabling function tracer
userns : don ' t leak root user
arch / avr32 : Eliminate NULL test and memset after alloc_bootmem
tproxy : Add missing CAP_NET_ADMIN check to ipv6 side
Btrfs : fix off - by - one in cow_file_range_inline ()
cifs : return error on invalid value written to cifsFYI
aio : prevent double free in ioctx_alloc
genirq , ARM : Fix boot on ARM platforms
hamradio / yam : fix info leak in ioctl
ARM : S3C64XX : Reduce residency requirement for cpuidle WFI mode
trace : introduce missing mutex_unlock ()
igmp : Avoid zero delay when receiving odd mixture of IGMP queries
ASoC : SSM2602 : Add entry for the ssm2603 to the device id table
Staging : bcm : add a missing break statement
ext4 : fix error paths in swap_inode_boot_loader ()
[ SCSI ] qla2xxx : Correct report - id acquisition check
tridentfb : use display information in info not in var for panning
video : vt8500lcdfb : Remove kfree call since devm_kzalloc () is used
staging : fbtft : do not allocate huge txbuf
[ media ] davinci : dm355 : Fix uninitialized variable compiler warnings
crypto : caam - fix output sequence contiguity check
ALSA : seq : Fix missing NULL check at remove_events ioctl
floppy : Add an extra bound check on ioctl arguments
x86 : don ' t send SIGBUS for kernel page faults
ARM : 7185 / 1 : perf : don ' t assign platform_device on unsupported CPUs
e1000e : static analysis tools complain of a possible null ptr p dereference
NFC : Clear pn533 target structure
HID : cp2112 : fix gpio - callback error handling
eCryptfs : Unlock page in write_begin error path
USB : EHCI : add a name for the platform - private field
ibm_newemac : Fixes entry of short packets
scsi : Fix use - after - free
mm : Adjust do_pages_stat () so gcc can see copy_from_user () is safe
staging : ozwpan : Return error , if PD is not connected .
dp83640 : Get calibration pin with ptp_find_pin
cdrom : fix improper type cast , which can leat to information leak .
ASoc : wm8731 : add 32bit mode .
fm10k : Fix a potential NULL pointer dereference
x86 / fpu : Don ' t cache access to fpu_fpregs_owner_ctx
greybus : bundle : use hexadecimal notation for class attribute
rtlwifi : rtl8192ce : Set fw_ready flag
ibmvnic : Handle error case when setting link state
cdrom : Check SCSI passthrough support before reading audio
batman - adv : fix potential orig_node reference leak
perf hists browser : Fix segfault if use symbol filter in cmdline
tracing : Have error path in predicate_parse () free its allocated memory
ipv4 : fib : Set offload indication according to nexthop flags
ARM : OMAP : rx51 : Fix a section mismatch warn
cfg80211 : ignore invalid BSSIDs when looking for BSSes
usb : chipidea : usb2 : check memory allocation failure
USB : allow usbstorage to have LUNS greater than 2Tb
HID : i2c - hid : prevent buffer overflow in early IRQ
epoll : clear the tfile_check_list on - ELOOP
btrfs : send : remove BUG from process_all_refs
memstick : init sysfs attributes
[ media ] sh_veu : v4l2_dev wasn ' t set
iio : mxs - lradc : fix buffer overflow
pNFS / flexfiles : missing error code in ff_layout_alloc_lseg ()
staging : vt6655 : device_main : Replace NULL comparison with ! x
crypto : hash - Add AHASH_REQUEST_ON_STACK
drm / ttm : fix memory leak while individualizing BOs
cifs : ensure that vol -> username is not NULL before running strlen on it
USB : usbmon : use the new usb debugfs directory
soc : qcom : aoss : Fix missing put_device call in qmp_get
qla2xxx : Wait for reset completion without lock for ISPFX00 .
drm / amdkfd : Pass queue type to pqm_create_queue ()
staging : iio : dac : ad5686 : Check for negative values
ALSA : hda - fix the mic mute led problem for Latitude E5550
drm / amd / display : Disable cursor on video surface .
drm / nouveau / ltc / gv100 : initial support
ALSA : usb - audio : avoid freeing umidi object twice
cpufreq : Prevent problems in update_policy_cpu () if last_cpu == new_cpu
ALSA : snd - usb : Fix uninitialized variable usage
tty : goldfish : Fix a parameter of a call to free_irq
strip : Fix termios assumption
regmap : Check readable regs in _regmap_read
ALSA : timer : Fix leak in events via snd_timer_user_tinterrupt
[ media ] hdpvr : Fix memory leak
char / tpm : Fix unitialized usage of data buffer
phy : qcom - qusb2 : Fix crash if nvmem cell not specified
drm / vmwgfx : Fix a buffer object leak
staging : comedi : cb_pcidas : check for failure of subdev_8255_init
mac80211_hwsim : require at least one channel
x86 , efi : Check table header length in efi_bgrt_init ()
ALSA : ali5451 : Fix out - of - bound position reporting
iwlwifi : nvm : don ' t print NVM section reads by default
omap3 : Fix booting if package is uninitialized
dmaengine : bcm - sba - raid : serialize dma_cookie_complete () using reqs_lock
KVM : Don ' t accept obviously wrong gsi values via KVM_IRQFD
ASoC : atmel_ssc_dai : Match the CMR divider only in full duplex .
ip6gretap : Fix MTU to allow for Ethernet header
xfrm_user : fix info leak in copy_to_user_policy ()
rpmsg : char : release allocated memory
brcmfmac : clear eventmask array before using it
staging : hv : fix counting of available buffer slots when send fails
USB : usbfs : fix - ENOENT error code to be - ENODEV
[ media ] ov5642 : avoid calling ov5642_find_datafmt () twice
block : handle blk_pm_resume_request () requests in blk_execute_rq_nowait ()
hv_netvsc : fix a race between netvsc_send () and netvsc_init_buf ()
dmaengine : ioatdma : workaround SKX ioatdma version
Yama : handle 32 - bit userspace prctl
crypto : api - Fix crypto_drop_spawn crash on blank spawns
powerpc / powernv : Initialize DMA for PEs
xhci : really enqueue zero length TRBs .
drivers / usb / host / ehci - fsl . c : add missing iounmap
perf tools : Fix a memory leak in __open_dso ()
mac80211 : initialize power mode for mesh STAs
Input : ad7877 - filter events where pressure is beyond the maximum
cdrom : Fix info leak / OOB read in cdrom_ioctl_drive_status
staging : rtl8723au : collect_bss_info (): Don ' t memset () what we just kzalloc ()' d
KEYS : Fix termination condition in assoc array garbage collection
virtio_net : zero is an invald queue_pairs number
drm / amd / display : Add same check as reset pipes for programing backend regs .
watchdog : Fix null pointer dereference while accessing rdc321x platform_data
misc : apds9802als : Fix the logic checking timeout in als_wait_for_data_ready ()
ARM : 6076 / 1 : SA1100 : add processor check to sa1110 - cpufreq driver
fscrypt : fix dereference of NULL user_key_payload
perf annotate : Handle lower case key code in annotate_browser__run ()
ath10k : fix WMI scan command length
fix irq flags in saa7134
[ SCSI ] pmcraid : reject negative request size
r8169 : switch to napi_schedule_irqoff
[ PATCH ] uniput - fix crash on SMP
sched : re - tune NUMA topologies
kernel / signal . c : avoid undefined behaviour in kill_something_info
ufs_extend_tail (): fix the braino in calling conventions of ufs_new_fragments ()
aio : mark AIO pseudo - fs noexec
af_unix : don ' t append consumed skbs to sk_receive_queue
staging : mt7621 - pinctrl : add SPDX identifier
staging : comedi : jr3_pci : change module description
scsi : libsas : fix memory leak in sas_smp_get_phy_events ()
[ S390 ] hw_random : allow rng_dev_read () to return hardware errors .
rt2x00 : rt2800pci : use rt2x00dev -> tx -> limit
ath9k_hw : Fix tx power settings for AR9003
gpio : davinci : Fix possible NULL pointer deference
rtl8xxxu : Temporarily disable 8192eu device init
drm / i915 : Fix user ptr check size in eb_relocate_vma ()
libata : ensure host is free ' d on error exit paths
iwlwifi : set size of ucode section
staging : slicoss : fix a leak when kzalloc fail
ACPICA : Fix memory leak on unusual memory leak
[ SCSI ] qla2xxx : Correct regression in relogin code .
drm / ttm : test for dma_address array allocation failure
Btrfs : add missing error handling to read_tree_block
ACPI : Add NULL pointer check in acpi_bus_start
ACPI : battery : Support for non - spec name for LiIon technology
staging : greybus : uart . c : change ' unsigned ' to ' unsigned int '
staging : wilc1000 : fixes potential null dereference ' wid . val '
qla2xxx : Clear loop_id for ports that are marked lost during fabric scanning .
bpf : ixgbe : Report bpf_prog ID during XDP_QUERY_PROG
ext4 : set h_journal if there is a failure starting a reserved handle
ax25 : Fix NULL pointer dereference in ax25_kill_by_device
drm / i915 : Clear crtc atomic flags at beginning of transaction
ALSA : pcm : Call kill_fasync () in stream lock
irqchip / mips - gic : Map to VPs using HW VPNum
usbdev : BKL pushdown
rndis_wlan : add return value validation
bnxt_en : Do not allow VF to read EEPROM .
staging : unisys : visorbus : visorchipset . c : Fix bug in parser_init_byte_stream .
hugetlb : fix race condition in hugetlb_fault ()
Page migration : Do not accept invalid nodes in the target nodeset
drm / amdgpu / gfx8 : specify which engine to wait before vm flush
AppArmor : fix oops in apparmor_setprocattr
fs / befs / linuxvfs . c : befs_iget (): remove unneeded initialization to NULL
cxgb3 : Missing rtnl lock in error recovery
usb : gadget : don ' t release an existing dev -> buf
clk : axi - clkgen : Remove sometimes impossible check
md : raid1 : kill warning on powerpc_pseries
ceph : prevent i_version from going back
ftrace : iter ctrl fix
drm : Complete moving rotation property to core
clk : hisilicon : hi3660 ： Fix potential NULL dereference in hi3660_stub_clk_probe ()
lpfc : Fix internal loopback failure .
drm / tilcdc : Take CRTC lock when calling tilcdc_crtc_disable ()
wl1271 : fix wl1271_spi driver name
xfs : fix inobt inode allocation search optimization
jbd2 : clear BH_Delay & BH_Unwritten in journal_unmap_buffer
scsi : fcoe : fix use - after - free in fcoe_ctlr_els_send
[ media ] [ v3 , media ] av7110 : check for negative array offset
hp - wmi : add return value checking for input_allocate_device ()
ALSA : hda - Add mic - boost controls to ALC662 / 663 auto configuration
io_uring / msg_ring : check file type before putting
x86 : not clear empty_zero_page again
[ media ] rtl28xxu : lower the rc poll time to mitigate i2c transfer errors
drm / nouveau / fbcon : fix NULL pointer access in nouveau_fbcon_destroy
Input : usb1400_ts - fix access to " device data " in resume function
tpm : Fix initialization of the cdev
fuse : fix uninitialized flags in pipe_buffer
mctp : defer the kfree of object mdev -> addrs
powerpc / radix : Update MMU cache
drivers / cpufreq / pcc - cpufreq . c : avoid NULL pointer dereference
HID : roccat : Fixed false dpi reporting when using osd event
phy : ti - pipe3 : Fix suspend / resume and module reload
x86 , mm , hugetlb : Add missing TLB page invalidation for hugetlb_cow ()
caif : Fix missing msg_namelen update in caif_seqpkt_recvmsg ()
fuse : check size of FUSE_NOTIFY_INVAL_ENTRY message
mfd : rtsx : Release IRQ during shutdown
sound / soc / fsl / p1022_ds . c : add missing of_node_put
[ S390 ] cio : Log the response from the unit check handler
soc / tegra : pmc : Guard against uninitialised PMC clock
udf : avoid info leak on export
ACPI : debugfs custom_method open to non - root
sysctl : Drop reference added by grab_header in proc_sys_readdir
btrfs : verbose error when we find an unexpected item in sys_array
PCI : Fix devm_pci_alloc_host_bridge () memory leak
fscrypto : add authorization check for setting encryption policy
libnvdimm , btt : check memory allocation failure
igb : add i211 to i210 PHY workaround
ALSA : HDA : Fixup Realtek headphone pin initialization
ath9k : reinitialize block ack window data when starting aggregation
ARM : OMAP : Use kcalloc () in omap_system_dma_probe ()
staging : gdm724x : redundant variables idProduct and idVendor
drm : i915 : Explicitly apply PWM config extracted from pwm_args
KVM : nVMX : Validate the IA32_BNDCFGS on nested VM - entry
block : cciss : fix information leak to userland
btrfs : send : use kvmalloc in iterate_dir_item
KEYS : prevent KEYCTL_READ on negative key
ipv6 : don ' t use tw net when accounting for recycled tw
x86 / mce / AMD : Make the init code more robust
vfio - pci : Add missing break to enable VFIO_PCI_ERR_IRQ_INDEX
phy : tegra : xusb : Fix return value of tegra_xusb_find_port_node function
iio / adc : ( max1363 ) Provide OF node information to iio device
staging : intel_sst : Fix memory leak
drm / i915 : Restore marking context objects as dirty on pinning
x86 / microcode / AMD : Drop redundant printk prefix
ring - buffer - benchmark : Fix the wrong type
iwlwifi : mvm : don ' t allow sched scans without matches to be started
xen / evtchn : Fix name of Xen event - channel device
staging : olpc_dcon : actually return the value of i2c_add_driver
input : pmic8xxx - pwrkey : Do not use mfd_get_data ()
HID : corsair : fix mapping of non - keyboard usages
drm / omap : verify that display x - res is divisible by 8
USB : serial : omninet : fix reference leaks at open
blackfin : bf609 : let clk_disable () return immediately if clk is NULL
dm ioctl : fix out of bounds array access when no devices
perf : Initialize callchains roots ' s childen hits
mx3fb : fix regression with uninitalized fb_info -> mm_lock mutex
ipv6 : fix icmp6_dst_alloc ()
dccp / tcp : do not inherit mc_list from parent
sky2 : print Optima chip name
[ ARM ] prevent the vmalloc cmdline argument from eating all memory
[ media ] solo6x10 : free DMA allocation when releasing encoder
pinctrl : s3c24xx : off by one in s3c24xx_eint_init ()
clk : gpio : Unlock mutex on error path
[ POWERPC ] spufs : Don ' t yield nosched context
drm / nv50 / disp : allow interlaced and doublescan modes on digital outputs
ARM : DRA7 / AM43XX : fix header definition for omap44xx_restart
pinctrl : sunxi : check clk_prepare_enable return value
[ XFS ] Fix a mutex_destroy diagnostic about a locked - mutex - on - destroy from
brcmfmac : initialize escan function pointer during scheduled scan
e1000e : NVM write protect access removed from SPT HW
mm : memcg : Correct unregistring of events attached to the same eventfd
[ PATCH ] Dynamic kernel command - line : m68k
usb : otg : twl4030 - usb : Fix unbalanced regulator disables at module removal
orangefs : avoid freeing a slot twice in wait_for_direct_io ()
staging : android : ion : Return an ERR_PTR in ion_map_kernel
aio : fix kernel memory disclosure in io_getevents () introduced in v3 . 10
ALSA : hdsp : allow firmware loading from inside the kernel
KVM : x86 : Improve thread safety in pit
drm / amd / amdgpu : fix over - bound accessing in amdgpu_cs_wait_any_fence
ext4 : avoid hang when mounting non - journal filesystems with orphan list
amdkfd : fence_wait_timeout () can be static
[ media ] cx23885 : Silence unknown command warnings
KVM : MMU : invalidate and flush on spte small -> large page size change
ALSA : ice1724 - Add PCI postint to reset sequence
ASoC : rt5677 : Reintroduce I2C device IDs
wl3501_cs : remove pedantic build warning
perf : Treat attr . config as u64 in perf_swevent_init ()
bnx2x : VF ndo sanity
ptp_kvm : probe for kvm guest availability
[ GFS2 ] re - support special inode
rds : prevent dereference of a NULL device
drm / i915 : Disable gpu reset on i965g / gm
uas : Fix bounds check in uas_find_endpoints
[ PATCH ] ppc64 : iSeries early printk breakage
drm / vc4 : Fix an integer overflow in temporary allocation layout .
ASoC : Intel : Skylake : Initialize NHLT table
ipv6 : sr : fix out - of - bounds access in SRH validation
[ libata ] ahci : Fix lack of command retry after a success error handler .
greybus : interface : fix double freeing of interface structure
Btrfs : fix missing error handler if submiting re - read bio fails
ALSA : hda - Make azx_attach_pcm_stream static
powerpc : Fix handling of alignment interrupt on dcbz instruction
USB : ehci : fix debugfs ' lpm ' permissions
drm / i915 : Disable tiling on IGDNG for now
staging : rtl8192u : check dev_alloc_skb () return value in fw_download_code ()
IPoIB : Do not print error messages for multicast join retries
qlcnic : Remove __QLCNIC_DEV_UP bit check to read TX queues statistics .
staging : unisys : visorbus : Check controlvm message payload size
KVM : x86 : Don ' t report guest userspace emulation error to userspace
clk : versatile : free icst on error return
mmc : meson - gx - mmc : Fix platform_get_irq ' s error checking
gpio : moxart : Use devm_gpiochip_add_data () for gpio registration
viafb : remove LVDS initialization
kvm : irqchip : fix memory leak
atmel_spi : reload RCR before TCR
Input : twl4030_keypad - pass correct pointer to free_irq ()
[ PATCH ] s390 : chps [] array too short
pata_atiixp : fix trivial indentation misalignment on if statement
sctp : not allow transport timeout value less than HZ / 5 for hb_timer
ocfs2 : fix memory leak in dlm_migrate_request_handler ()
drivers : misc : pass miscdevice pointer via file private data
xen / blkback : Update to use blkdev_get_by_dev instead of open_by_devnum .
usb : misc : legousbtower : Fix memory leak
mtd_blkdevs : Set the maximum discards size
sctp : better integer overflow check in sctp_auth_create_key ()
libata : initialize qc -> dma_dir to DMA_NONE
sctp : do not inherit ipv6_ { mc | ac | fl } _list from parent
xen / events : prevent calling evtchn_get on invalid channels
mac80211 : lock sdata in ieee80211_csa_connection_drop_work ()
xtensa : prevent arbitrary read in ptrace
drm / radeon / kms : Fix NULL ptr dereference
ieee802154 : ca8210 : prevent memory leak
gro : Fix kcalloc argument order
iwlwifi : dbg_ini : fix memory leak in alloc_sgtable
[ PATCH ] Audit : Fix the format type for size_t variables
driver core : fix device_create () error path
fs / compat_ioctl . c : VIDEO_SET_SPU_PALETTE missing error check
sh : fix copy_from_user ()
x86 : fix warning on 32 - bit non - PAE
io_uring : check that we have a file table when allocating update slots
ubifs : Use kmalloc_array ()
i2c : xiic : Remove the Addressed as slave interrupt
stmmac : don ' t return zero on failure path in stmmac_pci_probe ()
pinctrl : samsung : Handle memory allocation failure during wakeup banks init
ALSA : hda - Another Dell headset quirk
vfio - pci : Handle error from pci_iomap
sctp : keep fragmentation point aligned to word size
sctp : avoid BUG_ON on sctp_wait_for_sndbuf
atl2 : Disable unimplemented scatter / gather feature
staging : typec : tcpm : Check cc status before entering SRC_TRY_DEBOUCE
ext4 : fix memory leak in ext4_insert_range ()
loopback : off by one in tcm_loop_make_naa_tpg ()
percpu : use notrace variant of preempt_disable / preempt_enable
[ PATCH ] fs / smbfs / proc . c : fix data corruption in smb_proc_setattr_unix ()
ALSA : hda - Update automute / automic upon jack retasking
io_uring : fix uninitialized field in rw io_kiocb
scsi : sg : allocate with __GFP_ZERO in sg_build_indirect ()
[ media ] v4l : async : check for v4l2_dev in v4l2_async_notifier_register ()
mfd : rave - sp : Initialize flow control and parity of the port
mlxsw : spectrum_router : Fix error path in mlxsw_sp_router_init
tcp : drop SYN + FIN messages
drm / i915 : Do no use kfree () to free a kmem_cache_alloc () return value
Input : ad7879 - pass up error codes from probe functions
igb : Fix a memory leak in igb_probe
chipidea : udc : free pending TD at removal procedure
bluetooth : Validate socket address length in sco_sock_bind ().
hwmon : Register thermal zone only if ' dev ' parameter was provided
reset : Fix of_reset_control_get () for consistent return values
i40e : prevent memory leak in i40e_setup_macvlans
fscrypto : only allow setting encryption policy on directories
tipc : remove unnecessary call to dev_net ()
x86 / microcode / intel : Guard against stack overflow in the loader
drivers : hamradio : 6pack : fix UAF bug caused by mod_timer ()
ASoC : imx - pcm - dma : check kzalloc return value in function snd_imx_open
powerpc / perf : Fix IMC initialization crash
drm / i915 : Expand DPF support to Haswell
staging : ozwpan : Check error condition before creating endpoint .
dm raid1 : keep retrying alloc if mempool_alloc failed
mwifiex : pcie : Fix memory leak in mwifiex_pcie_alloc_cmdrsp_buf
USB : pl2303 : fix return value of tiocmset
rtc : ac100 : Add NULL checking for devm_kzalloc call
ipv6 : Fix ip6_xmit to send fragments if ipfragok is true
[ media ] v4l2 - ioctl : set readbuffers to 2 in g_parm
mnt : release locks on error path in do_loopback
ath9k_hw : fix an off - by - one error in the PDADC boundaries calculation
[ POWERPC ] Improve robustness of the UIC cascade handler
[ XFS ] Make xfsbufd threads freezable
ASoC : Check for NULL register bank in snd_soc_get_cache_val ()
staging : frontier : use after free in disconnect ()
pinctrl / amd : Prepare amd_gpio_irq_handler for irq argument removal
powerpc / 64s / hash : Allow MAP_FIXED allocations to cross 128TB boundary
af_key : add __GFP_ZERO flag for compose_sadb_supported in function pfkey_register
usb : udc : core : fix error handling
[ SCSI ] scsi_lib : Fix bug in completion of bidi commands
powerpc / tm : Flush TM only if CPU has TM feature
i2400m : keep index within ms_to_errno []
iwlwifi : fix compilation warning when CONFIG_IWLWIFI_DEBUG is not set .
[ media ] usbtv : correctly handling failed allocation
tty / serial : at91 : fix race condition in atmel_serial_remove
drm / i915 / gvt : Do not use I915_NUM_ENGINES to iterate over the mocs regs array
[ PATCH ] cpufreq_conservative : alter default responsiveness
[ S390 ] qdio : output queue stall on FCP and network devices
Btrfs : clear uptodate flags of pages in sys_array eb
staging : sbe - 2t3e3 : Fix possible reuse of freed memory in timer function
[ PATCH ] aoe 5 / 12 : don ' t try to free null bufpool
fsl / fman : MEMAC may use QSGMII PHY interface mode
staging : wilc1000 : Revert " staging : wilc1000 : remove spaces around '->'"
[ Bluetooth ] Ignore additional interfaces of BPA 100 / 105 devices
[ NETFILTER ]: nf_conntrack_pptp : fix NAT setup of expected GRE connections
iommu / io - pgtable - arm - v7s : Check for leaf entry before dereferencing it
Phonet : refuse to send bigger than MTU packets
watchdog : imx2_wdt : also set wdog -> timeout to new_timeout
staging : vt6655 : device_rx_srv check sk_buff is NULL
mixart : Add missing vmalloc . h include
drm / i915 : Correctly reject invalid flags for wait_ioctl
rcuclassic : fix compiler warning
USB : gadget : validate interface OS descriptor requests
mmc : dw_mmc : fix the wrong condition checking
af_iucv : exploit target message class support of IUCV
ALSA : hda - Add docking station support for another HP machine
drm / etnaviv : ( re -) protect fence allocation with GPU mutex
ASoC : soc - core : use GFP_KERNEL flag for kmalloc in snd_soc_cnew
xfs : disallow ro -> rw remount on norecovery mount
KVM : s390 : fix assumption for KVM_MAX_VCPUS
qed : Add missing stat for new isles
Staging : rtl8192u : Fix no spaces around '+'
staging : comedi : ni_labpc_cs : remove thisboard macro
[ SCSI ] scsi_scan . c : Release mutex in error handling code
[ PATCH ] swsusp : prevent disks from spinning down and up
powerpc / spufs : Remove invalid semicolon after if statement
tracing : Fix freeing of filter in create_filter () when set_str is false
PM : Free memory bitmaps if opening / dev / snapshot fails
[ PATCH ] sysfs : zero terminate sysfs write buffers
ipw2100 : declare MODULE_FIRMWARE
arch / tile : fix reversed test of strict_strtol () return value
rtnl : fix info leak on RTM_GETLINK request for VF devices
x86 : remove set_fixmap () warning
drm / i915 : Only flush fbc on sw when fbc is enabled .
ax25 : Fix UAF bugs in ax25 timers
drm / ttm : fix memory leak noticed by kmemleak .
[ SCSI ] ipr : dlpar failed when adding an adapter back
pcie port driver : correctly detect native PME feature
[ PATCH ] lockdep : annotate scheduler runqueue locks
drivers / net / phy / mdio_bus . c : call put_device on device_register () failure
xfs : use -> t_dfops in log recovery intent processing
[ PATCH ] stack overflow safe kdump : safe_smp_processor_id (): voyager
e1000e : do not attempt to disable smart powerdown if not supported
proc : prevent accessing / proc /< PID >/ environ until it ' s ready
rose : fix info leak via msg_name in rose_recvmsg ()
dac960 : return success instead of - ENOTTY
perf tools : Do not accept parse_tag_value () overflow
tpm : fix a kernel memory leak in tpm - sysfs . c
dm thin : restore requested ' error_if_no_space ' setting on OODS to WRITE transition
usb : phy : msm : cast to enum msm_usb_phy_type
dccp : Empty the write queue when disconnecting
crypto : user - fix memory leak in crypto_report
ASoC : sgtl5000 : Improve the error message on slave mode setting
eCryptfs : Remove buggy and unnecessary write in file name decode routine
ath9k_htc : Protect ampdu_action with a mutex
xfs : fix freeing memory in xfs_getbmap ()
iommu / amd : Check if domain is NULL in get_domain () and return - EBUSY
[ PATCH ] drivers cris : return on NULL dev_alloc_skb ()
usb : xhci : Fix OOPS in xhci error handling code
atm : fix info leak in getsockopt ( SO_ATMPVC )
ipv6 : sit : update mtu check to take care of gso packets
tty : drop the pty lock during hangup
drivers / net / eql . c : prevent reading uninitialized stack memory
vhost : fix length for cross region descriptor
drm / i915 : Defend against userspace creating a gem object with size == 0
KVM : x86 : inject exceptions produced by x86_decode_insn
brcmfmac : avoid potential stack overflow in brcmf_cfg80211_start_ap ()
tipc : validate length of sockaddr in connect () for dgram / rdm
cpufreq : Do not clear real_cpus mask on policy init
exofs : Remove redundant unlikely ()
fs_enet : Don ' t receive packets when the napi budget == 0
drm / msm : Call drm_prime_gem_destroy to clean up imported GEM object
USB : uas : Fix slave queue_depth not being set
[ PATCH ] KVM : Add missing ' break '
clk : tegra : Use definition for pll_u override bit
[ SCSI ] isci : fix missed unlock in apc_agent_timeout ()
[ media ] smsusb : fix the default delivery system setting
greybus : interface : fix potential attribute - buffer overflow
xfs : trivial indentation fixup for xfs_iext_remove_node
tg3 : Fix tg3_skb_error_unmap ()
crypto : inside - secure - fix an error handling path in safexcel_probe ()
KVM : x86 : Fix potential divide by 0 in lapic ( CVE - 2013 - 6367 )
powerpc / mm : Fix hash table dump when memory is not contiguous
wil6210 : fix check for sparrow D0 FW file
drm / sun4i : Fix uninitialized variables in vi layer
iommu / amd : Free domain id when free a domain of struct dma_ops_domain
blk - mq : fix allocation of set -> tags
[ media ] coda : v4l2 - compliance fix : zero pixel format priv field
perf : arm64 : Record the user - mode PC in the call chain .
coresight : etb10 : Move etb_disable_hw () outside of lock
USB : usbaudio : handle kcalloc failure
greybus : audio : return success for stop trigger if device removed
[ PATCH ] x86_64 : free_bootmem_node needs __pa in allocate_aperture
HID : picolcd : sanity check report size in raw_event () callback
ALSA : Fix a Oops bug in omap soc driver .
ARM : stm32 : Fix copyright
batman - adv : fix memory leak when dropping packet from other gateway
crypto : blkcipher - Do not use eseqiv on stream ciphers
mac80211 : properly free requested - but - not - started TX agg sessions
bnxt_en : Check for zero length value in bnxt_get_nvram_item ().
iommu : smar : Fix return value check of create_irq ()
drm / virtio : don ' t leak bo on drm_gem_object_init failure
brcmfmac : correct setting of WEP broadcast / unicast keys
drm / i915 : Drain any freed objects prior to hibernation
staging : ozwpan : prevent overflow in oz_cdev_write ()
drm / atomic : Make atomic helper track newly assigned planes correctly , v2 .
sched : reorder struct sched_rt_entity to remove padding on 64 bit builds
iio : health : max30102 : Check retval of powermode function
cifs : fix use - after - free caused by invalid pointer ` hostname `
dm flakey : check for null arg_name in parse_features ()
block : blk_init_allocated_queue () set q -> fq as NULL in the fail case
sctp : Fix sk_ack_backlog wrap - around problem
staging : unisys : avoid format string parsing
fm10k : don ' t try to stop queues if we ' ve lost hw_addr
xfs : fix xfs_error_get_cfg for negative errnos
staging : ozwpan : oz_pd_free () can be static
bonding : disallow enslaving a bond to itself
xhci : check for failed dma pool allocation
cpuidle : delay enabling interrupts until all coupled CPUs leave idle
rtc : allow validated RTC_PIE_ON for non - root
spi / ppc4xx : Use kcalloc () in spi_ppc4xx_of_probe ()
crypto : tcrypt - fix uninit sg entries in test_acipher_speed
ipv6 : do not free rt if FIB_LOOKUP_NOREF is set on suppress rule
sysctl : check length at deprecated_sysctl_warning
[ SCSI ] libsas : remove expander from dev list on error
ath10k : enable channel 144 on 5GHz band
sh64 : re - add the __strnlen_user () prototype
rtc : sun6i : ensure rtc is kfree ' d on error
[ SCSI ] zfcp : Fix error checking for ELS ADISC requests
jbd : Check return value of __getblk ()
video : fbdev : pxa3xx - gcu : Fix integer overflow in pxa3xx_gcu_write
cpufreq : st : enable selective initialization based on the platform
rpmsg : glink : Initialize the " intent_req_comp " completion variable
perf intel - pt : Fix missing stack clear
mwifiex : sdio : fix use after free issue for save_adapter
phy : exynos - mipi - video : avoid uninitialized variable use
mtd : nand : sharpsl : Register partitions using the parsers
drm / i915 / gvt : Disable access to stolen memory as a guest
[ PATCH ] readahead : reset cache_hit earlier
ALSA : sound / isa / opti9xx / miro . c : eliminate possible double free
USB : serial : upd78f0730 : sort device ids
mfd : don ' t use memzero
cuse : fix memory leak
mac80211 : Fix ibss station got expired immediately
ASoC : fsi : constify dev_pm_ops structure
spi : core : Fix check for dma_map_sg () failures
ext4 : Fix ext4_ext_journal_restart ()
btrfs : fix wrong max system array size check in kernel space
ALSA : HDA : Fix volume control naming for surround speakers on Realtek auto - parser
Btrfs : fix wrong extent mapping for DirectIO
Btrfs : fix return value from btrfs_listxattr when buffer size is too small
drm / tegra : Use lockless gem BO free callback
tools / liblockdep : Fix comparison of a boolean value with a value of 2
[ IPV6 ]: Fix slab corruption running ip6sic
jbd2 : preserve original nofs flag during journal restart
futex : Prevent overflow by strengthen input validation
ASoC : dapm : Make sure all dapm contexts are updated
ipw2200 : fix error return code in ipw_load ()
KVM : X86 : Don ' t use PV TLB flush with dedicated physical CPUs
fuse : destroy bdi on error
drm / bridge : adv7511 : Fix a use after free
ASoC : rockchip : i2s : fix unbalanced clk_disable
staging : gdm72xx : Condense two statements into one to improve code readability .
drm / nouveau / mmu / gp104 : initial support
usb : gadget : s3c2410_udc : Remove unneeded condition
staging / lustre / llite : Fix integer overflow in ll_fid2path
cxgb4vf : Fail open if link_start () fails .
Drivers : w1 : remove last __devexit_p () instance
mm : hugetlb : fix clear_huge_page ()
perf tools : Fix new_term () missing free on error path
drm / nouveau / fifo / gp104 : initial support
rxrpc : Fix oops when discarding a preallocated service call
sh : Fixed the TMU0 reload value on resume
usb : dwc3 : gadget : add a debugging print when initializing endpoints
mm : Introduce kvcalloc ()
Input : ams_delta_serio - fix wrong kfree in ams_delta_serio_exit
Btrfs : add missing compression property remove in btrfs_ioctl_setflags
ALSA : timer : Fix double unlink of active_list
panic , vt : do not force oops output when panic_timeout < 0
ASoC : cs42l52 : microphone bias is controlled by IFACE_CTL2 register .
microblaze : Remove duplicated prototype of start_thread ()
i2c - omap : Fix build breaking typo cpu_is_omap_2430
mac80211 : fix the assignment of mesh element TTL
[ XFS ] Fix up warning for xfs_vn_listxatt ' s call of list_one_attr () with
mac80211 : use RCU read locks for sta_info_get
mdio_bus : Fix use - after - free on device_register fails
mwifiex : fix a reversed condition
usb : otg : mxs - phy : add missing type to usb_phy
usb : gadget : fsl : fix the fault issue on rmmod
rapidio / tsi721 : fix incorrect detection of address translation condition
staging : lustre : llite : default dir stripe index only for mkdir
ASoC : qcom : ipq806x : add error in dma allocation .
ASoC : sst_platform : free the resources on fail path
staging : sep : add missing misc_deregister () on error in sep_register_driver_with_fs ()
mfd : palmas : Initialise client -> of_node for dummy created client
virt : vbox : Only copy_from_user the request - header once
ath10k : fix firmware assert in monitor mode
[ PATCH ] Char : serial167 , remove useless tty check
pinctrl : sx150x : handle missing ' advanced ' reg in sx1504 and sx1505
x86 : fix pci - gart failure handling
tpm_infineon : remove use of __devexit_p
ima : fix add LSM rule bug
[ CIFS ] fix trailing whitespace
KVM : Align cr8 threshold when userspace changes cr8
igb : add conditions for I210 to generate periodic clock output
PCI : Don ' t leak memory if sysfs_create_bin_file () fails
soc / tegra : pmc : Ensure mutex is always initialised
llc : better deal with too small mtu
KVM : x86 : fix NULL deref in vcpu_scan_ioapic
igb : Fix Null - pointer dereference in igb_reset_q_vector
Btrfs : fix oops when btrfs_inherit_iflags called with a NULL dir
[ media ] vivi : set device_caps
ASoC : Return correct codec clock in s3c64xx - i2s
sysctl : Add neg_one as a standard constraint
AFS : Fix afs_prepare_write ()
[ media ] saa7164 : better handle error codes
tcp : ipv6 : bind () use stronger condition for bind_conflict
i40iw : Add NULL check for ibqp event handler
xen : events : do not free legacy IRQs
crypto : ixp4xx - don ' t leak pointers to authenc keys
ALSA : timer - Fix Oops at closing slave timer
drm / i915 / gvt : add enable_execlists check before enable gvt
greybus : module : name routines consistently
bridge : netfilter : fix information leak
staging : iio : iio_ring_rip_outer return immediately if rip_lots returns <= 0
sysfs : fix hardlink count on device_move
UDF : Close small mem leak in udf_find_entry ()
arm64 : Fix building error with 16KB pages and 36 - bit VA
sched : Guarantee new group - entities always have weight
ACPI : Remove acpi_pci_slot_init () headers from internal . h
ACPI : add AC / DC notifier
ASoC : Fix register cache initialisation for WM8753
e100 : make sure vlan support isn ' t advertised on old adapters
fs / 9p : destroy fid on failed remove
mac80211 : Fix frame injection using non - AP vif
memcg : fix wrong check of noswap with softlimit
drm / amd / display : Set irq state only on existing crtcs
drm / i915 : use DPLL_DVO_HIGH_SPEED for PCH eDP
NVMe : End sync requests immediately on failure
tools : iio : generic_buffer : free dev_dir_name on exit
f2fs : check range before defragment
mac80211 : fix rate control initialisation
bnx2x : fix possible panic under memory stress
asus - laptop : fix a memory leak in asus_laptop_get_info error path
Input : synaptics - add manual min / max quirk for ThinkPad X240
USB : ehci : add call of free_cached_itd_list () function in disable_periodic ()
drivers / atm / eni . c : ensure arguments to request_irq and free_irq are compatible
drm / i915 : Clear intel_crtc -> atomic before updating it .
nvmet - fc : ensure target queue id within range .
ASoC : wm_adsp : Fix support for firmware switching on DSP2 and higher
greybus : core : add defensive connection disable post disconnect
cifs : Silence uninitialized variable warning
kvm : x86 : make function static to avoid compiling warning
mm : vma_adjust : remove superfluous check for next not NULL
ALSA : echoaudio : check kmalloc () result
imx - drm : imx - ldb : Check ' mode ' pointer first
[ PATCH ] x86_64 : Don ' t confuse apic =... command line option with apic
leds : Use kcalloc instead of kzalloc to allocate array
ARC : fix build warnings with ! CONFIG_KPROBES
drm / i915 : A hotfix for making aliasing PPGTT work for GVT - g
perf / x86 / intel / uncore : Correct fixed counter index check in generic code
spi : imx : Gracefully handle NULL master -> cs_gpios
dmaengine : sun6i - dma : Only calculate residue if state exists .
bnx2x : Add known PHY type check
seq_file : disallow extremely large seq buffer allocations
lightnvm : fail fast on passthrough commands
btrfs : Check name_len before reading btrfs_get_name
dmaengine : k3dma : Off by one in k3_of_dma_simple_xlate ()
staging : wilc1000 : linux_wlan . c : add kzalloc error check
ath9k : release allocated buffer if timed out
acornfb : Dont BUG () on invalid pan parameters
[ IPV6 ]: Address autoconfiguration does not work after device down / up cycle
Input : don ' t modify the id of ioctl - provided ff effect on upload failure
fbcon : when font is freed , clear also vc_font . data
scsi : qedi : Fix a potential buffer overflow
drm / mediatek : don ' t use drm_put_dev
fs : Add a missing permission check to do_umount
[ MTD ] MTD_DEBUG always does compile - time typechecks
[ SCSI ] megaraid_sas : remove sysfs dbg_lvl world writeable permissions
crypto : ccp - Release all allocated memory if sha type is invalid
xhci : replace USB_MAXINTERFACES with config -> desc . bNumInterface
spi : gpio : prevent memory leak in spi_gpio_probe
drm / i915 : release mutex in i915_gem_init ()' s error path
s390 / cio : add NULL test
staging : lustre : fix sparse warning about different address spaces
ALSA : HDA : Fix Oops caused by dereference NULL pointer
ARM : 6891 / 1 : prevent heap corruption in OABI semtimedop
drm / i915 / bxt , glk : Fix assert on conditions for DC9 enabling
tracing / filter : Add missing initialization
drm : clean dirty memory after device release
x86 / intel_rdt : Remove redundant ternary operator on return
sysfs : Make poll behaviour consistent
staging : dgap : Add check for MAXBOARDS in . probe ( dgap_init_one )
drivers / perf : arm_pmu_acpi : Release memory obtained by kasprintf
sch_sfb : Fix missing NULL check
ASoC : Intel : Skylake : check manifest size
ASoC : s6000 : Use WARN_ON () instead of BUG_ON ()
[ media ] drivers / media / dvb - frontends / stv0900_sw . c : Fix break placement
rt2x00 : Don ' t count retries as failure
ath9k_hw : fix synth delay for half / quarter channels
apparmor : fix update the mtime of the profile file on replacement
usb : return error code when platform_get_irq fails
perf trace / scripting : Check return val of perl_run ()
qlcnic : Fix function return error check
atm : fix info leak via getsockname ()
perf / core : Update filters only on executable mmap
sched : Remove unlikely () from ttwu_post_activation
tracing : Fix off - by - one in trace_get_user ()
proc : fix null pointer deref in proc_pid_permission ()
[ S390 ] add atomic64_xchg to s390
mfd : sta2x11 - mfd : Only add sta2x11_mfd if it hasn ' t already been added
staging : vt6656 : check ieee80211_bss_conf bssid not NULL
x86 / cpu , x86 / pti : Do not enable PTI on AMD processors
inotify : stop kernel memory leak on file creation failure
ethtool : EEPROM dump no longer works for tg3 and natsemi
PCI : Use cpu_hotplug_disable () instead of get_online_cpus ()
[ media ] vb2 : fix compiler warning
debugfs : set no_llseek in DEFINE_DEBUGFS_ATTRIBUTE
ALSA : als4000 : enable burst mode
[ SCSI ] sr : remove dead code
ip_tunnel : clear IPCB in ip_tunnel_xmit () in case dst_link_failure () is called
Btrfs : Fix test_range_bit for whole file extents
[ NETFILTER ]: nfnetlink_log : fix NULL pointer dereference
drm : fixes when i2c encoder slave mode_fixup is null .
evm : checking if removexattr is not a NULL
ASoC : wm8753 : remove invalid free of devm_ allocated data
vmwgfx : Unreference surface on cursor error path
ASoC : Intel : Fix incorrect sizeof () in sst_hsw_stream_get_volume ()
mwl8k : prevent freeing free IRQ if ieee80211_register_hw () fails
ASoC : Intel : Skylake : Initialize module list for Broxton
kvm : x86 : fix kvm_apic_has_events to check for NULL pointer
UWB : Return UWB_RSV_ALLOC_NOT_FOUND rather than crashing on NULL dereference if kzalloc fails
xfrm : Fix kernel panic when flush and dump SPD entries
crypto : atmel - aes - add new version
[ PATCH ] HID read busywait fix
brcmfmac : Add check for short event packets
lib : rhashtable : Correct self - assignment in rhashtable . c
ASoC : Intel : Skylake : Fix the module state check condition
ARM : S5P64X0 : Replace irq_gc_ack () with irq_gc_ack_set_bit ()
[ media ] c8sectpfe : Fix broken circular buffer wp management
mmc : core : Add card_busy to host_ops
wl1271 : Fix mac80211 RTS threshold requests during WL1271_STATE_OFF
orinoco : Maintain lock until entry removed from list
video : mmp : fix memcpy wrong size for mmp_addr issue
iio : gyro : ADXRS450 : Add missing read support for calibbias attribute
Staging : otus : hpfwu . c : Checkpatch cleanup
staging : wilc1000 : add check for kmalloc allocation failure .
make macfb_setup () static
[ SCSI ] libsas : Warn if ATA device detected but CONFIG_SCSI_SAS_ATA not set
batman - adv : Print jiffies as unsigned long
sock_diag : Fix out - of - bounds access to sock_diag_handlers []
tipc : fix an infoleak in tipc_nl_compat_link_dump
x86 / ioapic : Prevent NULL pointer dereference in setup_ioapic_dest ()
KEYS : fix out - of - bounds read during ASN . 1 parsing
[ SCSI ] remove unnecessary scsi_delete_timer () call in scsi_reset_provider ()
iio : adc : set INPUT_PROP_DIRECT
cifs : potential missing check for posix_lock_file_wait
scsi : fnic : Zero io_cmpl_skip on fw reset completion
rpmsg : virtio : Fix possible double free in rpmsg_virtio_add_ctrl_dev ()
rxrpc : Validate the net address given to rxrpc_kernel_begin_call ()
ieee1394 : node manager causes up to ~ 3 . 25s delay in freezing tasks
dm crypt : fix free of bad values after tfm allocation failure
ixgbe : Fix FCOE memory leak for DDP packets
[ agp ]: fixup chipset flush for new Intel G4x .
crypto : authenc - Use correct ahash complete functions
fuse : fix SEEK_END incorrectness
mm : numa : Do not account for a hinting fault if we raced
drm / radeon / kms : Don ' t kzalloc memory which is immediately overwritten .
device property : export irqchip_fwnode_ops
scsi : scsi_transport_iscsi : fix the issue that iscsi_if_rx doesn ' t parse nlmsg properly
mv643xx_eth : check for valid hw address ( resubmit )
pinctrl : imx : off by one in imx_pinconf_group_dbg_show ()
f2fs : skip writing in -> writepages when no dirty pages exist
iwlwifi : mvm : comply with fw_restart mod param on suspend
CacheFiles : Fix incorrect test for in - memory object collision
drm / radeon : fix UVD 256MB check
OMAP3 : SRAM size fix for HS / EMU devices
btrfs : fix missing last - entry in readdir ( 3 )
ASoC : Hold client_mutex while calling snd_soc_instantiate_cards ()
HID : logitech : fix bounds checking on LED report size
drivers / net / pcmcia / 3c589_cs : fix port configuration switcheroo
UDF : Fix a null pointer dereference in udf_sb_free_partitions
drm / bridge : synopsys : dw - hdmi : clean up drm_bridge_add call
xen - gntalloc : integer overflow in gntalloc_ioctl_alloc ()
mtd : orion_nand : Fix build failure caused by typo
iucv : Fix missing msg_namelen update in iucv_sock_recvmsg ()
fs : logfs : Fix up MTD = y build .
arm / arm64 : KVM : Load the timer state when enabling the timer
hfsplus : fix Buffer overflow with a corrupted image
[ PATCH ] aacraid : swapped kmalloc args .
spi : zynq : missing break statement
isicom : Deinline drop_dtr , save 112 bytes
Btrfs : make sure btrfs_remove_free_space doesn ' t leak EAGAIN
[ media ] pvrusb2 : don ' t go past buf array
Btrfs : fix memory leak in btrfs_ioctl_start_sync ()
Btrfs : Don ' t error on resizing FS to same size
PCI : tegra : Fix host bridge memory leakage
perf / core : Fix the perf_cpu_time_max_percent check
KVM : PPC : Fix oops when checking KVM_CAP_PPC_HTM
[ IPV4 ] fib_hash : fix duplicated route issue
drm / vc4 : Return - EINVAL on the overflow checks failing .
[ PATCH ] epca : update . owner field of struct pci_driver
drm / atmel - hlcdc : check stride values in the first plane
[ media ] davinci : vpbe : fix build error when CONFIG_VIDEO_ADV_DEBUG is enabled
rtc : m41t80 : add wakealarm functionality
stmmac : Remove unbalanced clk_disable call
hpfs : fix warnings when the filesystem fills up
arch / tile : check kmalloc () result
staging : wilc1000 : fix parameter name of function declaration
drm / nouveau / disp / nv04 : avoid creation of output paths
mfd : arizona : Check errors from regcache_sync ()
btrfs : check file extent backref offset underflow
iptunnel : make rx / tx bytes counters consistent
dmaengine : Stricter legacy checking in dma_request_slave_channel_compat ()
Input : sur40 - fix error return code
rtlwifi : fix memory leak for USB device
mmc : core : Fix use of uninitialized data in mmc_send_if_cond .
HID : hid - picolcd : Fix memory leak in picolcd_debug_out_report ()
ath6kl : Support net_stats . multicast
IPoIB : Fix crash in path_rec_completion ()
linux / types . h : Use __u64 for aligned_u64
slub : Zero initial memory segment for kmem_cache and kmem_cache_node
iwlwifi : mvm : check if returned value is NULL
ipmi : info leak in compat_ipmi_ioctl ()
bpf : fix double - fdput in replace_map_fd_with_map_ptr ()
[ SCSI ] be2iscsi : memset wrb for ring create
Input : twl6040 - vibra - do not reparent to grandparent
drm / i915 : Don ' t try to calculate RC6 residency on GEN4 and before
gpio : GPIO_GET_CHIPINFO_IOCTL : Fix information leak
Btrfs : Add error handle for btrfs_search_slot () in btrfs_read_chunk_tree ()
fib_semantics : Don ' t match route with mismatching tclassid
scsi : ufs : ufs - mediatek : Fix error checking in ufs_mtk_init_va09_pwr_ctrl ()
sunrpc : Avoid a KASAN slab - out - of - bounds bug in xdr_set_page_base ()
parisc / PCI : Set type for LBA bus_num resource
drm / edid : Don ' t skip every eighth EST III mode
GFS2 : O_TRUNC not working on stuffed files across cluster
platform / x86 : intel_ips : Use PCI_VDEVICE () macro
powerpc / cell : Fix integer constant warning
rxrpc : Make service connection lookup always check for retry
HID : hid - cypress : validate length of report
scsi : lpfc : Fix NULL pointer dereference during PCI error recovery
ax25 : fix NPD bug in ax25_disconnect
inet : properly align icsk_ca_priv
perf / core : Fix perf_uprobe_init ()
[ PATCH ] ieee1394 : fix cross_bound check for null ISO packets
crypto : md5 - Set statesize
include / linux / poison . h : fix LIST_POISON { 1 , 2 } offset
perf inject : Do not repipe attributes to a perf . data file
ath5k : free sc -> ah in ath5k_deinit_softc
mac80211 : read station mgmt keys via get_key call
fs / select . c : fix information leak to userspace
[ media ] s5c73m3 : Fix entity function assignment for the OIF subdev
KEYS : ensure we free the assoc array edit if edit is valid
powerpc / perf : Fix crash in perf_instruction_pointer () when ppmu is not set
tty : Fix LED error return
ovl : Return - ENOMEM if an allocation fails ovl_lookup ()
[ MIPS ] Fix oprofile module unloading
btrfs : fix NULL pointer dereference when deleting device by invalid id
wil6210 : missing reinit_completion in HALP voting
ocfs2 : free path in ocfs2_remove_inode_range ()
ipv6 : rt6_info output redirect to tunnel output
drm / amd / display : Correct fixed point calculation .
module : don ' t call percpu_modfree on NULL pointer .
PCI : xilinx : Dispose of MSI virtual IRQ
scsi : sg : reset ' res_in_use ' after unlinking reserved array
clk : make sure parent is not NULL in clk_fetch_parent_index ()
ASoC : imx - ssi : add check on platform_get_irq return value
drm / exynos : fix flags in dma buf exporting
ASoC : topology : Add support for compressed PCMs
drm / i915 : Clear the pending_gpu_fenced_access flag at the start of execbuffer
spi : qup : Fix return value checking for pm_runtime_get_sync ()
Staging : speakup : free after input_unregister_device ()
libata piix3 support warning fix
staging : fsl - dpaa2 / eth : Check if notification rearm is successful
staging : comedi : das16m1 : check for subdev_8255_init () failure
wl12xx_sdio : set interrupt as wake_up interrupt
drbd : abort drbd_start_resync if there is no connection
xen / event : validate irq before get evtchn by irq
x86 / topology : Handle CPUID bogosity gracefully
mfd : db8500 : Update cpufreq device name
[ PATCH ] drivers / message / i2o / pci . c : fix a NULL pointer dereference
memcg : fix mem_cgroup_out_of_memory () return value .
VSOCK : Fix missing msg_namelen update in vsock_stream_recvmsg ()
drm / amd / display : fix gamma setting
RDMA : Fix goto target to release the allocated memory
Staging : wilc1000 : Prefer eth_broadcast_addr over memset ()
stmmac : fix noderef . cocci warnings
cfg80211 : fix indentation
shm : optimize exit_shm ()
Fix a dumb typo - use of & instead of &&
mmc : sdhci : fix low memory corruption
drm / i915 : Cancel pending execlists irq handler upon idling
usb : message : Fixed parenthesis error in sizeof function .
msm : timer : fix missing return value
drm / i915 : Initialize return value for empty i915_gem_object_unbind ()
batman - adv : remove obsolete skb_reset_mac_header () in batadv_bla_tx ()
powerpc / perf : Cap 64bit userspace backtraces to PERF_MAX_STACK_DEPTH
HID : Add mapping of new KEY_MEDIA_REPEAT keycode
ixgbe : use kzalloc for allocating one thing
nfsd : clean up reset_union_bmap_deny
rds : fix an infoleak in rds_inc_info_copy
[ media ] dvb : si21xx : buffer overflow in si21_writeregs ()
usb : storage : return on error to avoid a null pointer dereference
RDS : verify the underlying transport exists before creating a connection
x86 : ptrace , bts : fix an unreachable statement
vmscan : promote shared file mapped pages
wanxl : fix info leak in ioctl
drm / tegra : Correct timeout in tegra_syncpt_wait
isdn : avoid copying too long drvid
USB : yurex : Fix missing URB_NO_TRANSFER_DMA_MAP flag in urb
perf / x86 : Correctly use FEATURE_PDCM
[ MIPS ] klconfig add missing bracket
btrfs : memory leak in btrfs_add_inode_defrag ()
Btrfs : avoid warning bomb of btrfs_invalidate_inodes
cpufreq : sfi : Don ' t validate the frequency table twice
drm : bochs : Prevent double - free of fb helper
genirq : Fix type inconsistency
i7core_edac : do not export static functions
staging : most : fix sparse warning Using plain integer as NULL pointer
[ PATCH ] USB : check for device in zd1201_resume
ALSA : hda - Skip card registration when no codec is found
[ PATCH ] ISDN : Avoid a potential NULL ptr deref in ippp
PCI : pciehp : disable DLL state changed event notification
mfd : Fix more undefined twl4030 - power resconfig value checks
Staging : vt6656 : checkpatch : fix identifier name in wcmd . h
ipv4 : fix debug info in tnode_new
wimax : i2400 : Fix memory leak in i2400m_op_rfkill_sw_toggle
iscsi - target : missing kfree () on error path
reiserfs : Fix softlockup while waiting on an inode
mac80211 : Clear wowlan flag when drv_suspend returns failure
Blackfin : debug - mmrs : disable PERIPHERAL_MAP for IMDMA channels
[ media ] solo6x10 : Fix solo_eeprom_read retval type
bpf : devmap fix arithmetic overflow in bitmap_size calculation
simple_xattr : permit 0 - size extended attributes
mtd : atmel_nand : increase chip_delay
KVM : use after free in kvm_ioctl_create_device ()
signal : Fix name of SIGEMT in # if defined () check
tpm_crb : check for bad response size
USB : io_ti : Fix NULL dereference in chase_port ()
sched : suppress RCU lockdep splat in task_fork_fair
staging : comedi : das1800 : prefer kmalloc_array over kmalloc with multiply
drm / i915 : Avoid NULL ctx -> obj dereference in debugfs / i915_context_info
UBIFS : replace count * size kzalloc by kcalloc
qla2xxx : Update entry type 270 to match spec update .
crypto : shash - Fix unaligned calculation with short length
KEYS : potential uninitialized variable
UBI : Silence an unintialized variable warning
arm / arm64 : KVM : Don ' t allow creating VCPUs after vgic_initialized
drm / i915 / userptr : reject zero user_size
lockdep : fix overflow in the hlock shrinkage code
ipx : call ipxitf_put () in ioctl error path
dccp : do not send reset to already closed sockets
pinctrl / amd : Drop pinctrl_unregister for devm_ registered device
rds : memory leak in __rds_conn_create ()
clk : st : Fix memory leak
ipv6 : fix devconf after adding force_tllao option
usb : chipidea : Fix memleak for ci -> hw_bank . regmap when removal
[ SCSI ] megaraid : Use the proper type to hold the irq number .
staging : comedi : unionxx5 : fix memory leak during subdevice init
crypto : algif_skcipher - sendmsg SG marking is off by one
staging : comedi : cb_das16_cs : return ' 0 ' for successful attach
farsync : fix info leak in ioctl
tg3 : Update version to 3 . 109
[ ALSA ] aoa : platform function gpio : ignore errors from functions that don ' t exist
perf / core : Fix potential double - fetch bug
vfs : rename : check backing inode being equal
ocfs2 : Fix xattr check in ocfs2_get_xattr_nolock ()
tty : release_one_tty () forgets to put pids
amd_iommu : do not dereference a NULL pointer address .
staging : usbip : vhci_hcd : fixed suspend - resume loop
vfs : extend vfs_removexattr locking
misc : mic : Fix for double fetch security bug in VOP driver
greybus : audio_codec : Free gccodec on codec probe failure
video : goldfishfb : fix memory leak on driver remove
TTY : drop driver reference in tty_open fail path
iio : inkern : allocate zeroed memory
scsi : bfa : release allocated memory in case of error
drbd : Becoming sync target may not happen out of < C_WF_REPORT_PARAMS
writeback : avoid unnecessary determine_dirtyable_memory call
drm / i915 : enable HDMI mode on VLV when an HDMI sink is detected
fbdev : sm501fb : use memset_io
watchdog : davinci : Handle return value of clk_prepare_enable
[ POWERPC ] arch / powerpc / kernel / vio . c of_node_put cleanup
wil6210 : Improve AP stop handling
[ POWERPC ] EEH : hotplug recovery bugfix
fix grammar in printk
sched : fix memory leak in a failure path
ALSA : compress_core : integer overflow in snd_compr_allocate_buffer ()
usb : dwc2 : avoid leaking DMA channels on disconnection
ozwpan : divide - by - zero leading to panic
pinctrl : sunxi : Enforce the strict mode by default
[ PATCH ] moxa serial : add proper capability check
hwmon : ( emc1403 ) Support full range of known chip revision numbers
cpufreq : SPEAr : clk_round_rate () can return a zero upon error
dlm : Fix kernel memory disclosure
drm / i915 : Don ' t overrun the intel_wa_regs array
[ PATCH ] Fix oops in vfs_quotaon_mount ()
ath9k_hw : reduce delay on programming INI on AR9003
drm / virtio : Fix NULL vs IS_ERR checking in virtio_gpu_object_shmem_init
vfs : mnt_want_write_file (): fix special file handling
EDAC : Respect operational state in edac_pci . c
Btrfs : only do the tree_mod_log_free_eb if this is our last ref
ipv4 : check optlen for IP_MULTICAST_IF option
HID : enable hid device to suspend / resume asynchronously
kernel / signal . c : stop info leak via the tkill and the tgkill syscalls
libnvdimm , pmem : fix persistence warning
x86 / irq : Fix move_irq_desc () for nodes without ram
[ media ] pulse8 - cec : set correct Signal Free Time
ksmbd : fix use - after - free bug in smb2_tree_disconect
cxgb4 : Inform caller if driver didn ' t upgrade firmware
staging : ks7010 : change some local variables type in ks_sdio_interrupt
[ S390 ] cio : path_event overindication after resume
[ PATCH ] pcnet32 : remove incorrect pcnet32_free_ring
f2fs : fix a dead loop in f2fs_fiemap ()
staging : rtl8188eu : Use kcalloc instead of kzalloc .
[ PATCH ] uml : switch_mm fix
sfc : don ' t BUG_ON efx -> max_channels == 0 in probe
drm / amdgpu_gem : fix error handling path in amdgpu_gem_va_update_vm
netrom : fix info leak via msg_name in nr_recvmsg ()
ALSA : usb - audio : add DSD support for new Amanero PID
USB : visor : fix null - deref at probe
TG3 : limit reaches - 1
fbdev : propagate result of fb_videomode_from_videomode ()
[ media ] easycap : Fix mem leak in easycap_usb_probe ()
tools : ffs - aio - example : add missing wMaxPacketSize for HS descs
ide : fix LOCKDEP warning
samples / bpf : deal with EBUSY return code from sendmsg in xdpsock sample
ubi : Fix whitespace issue in count_fastmap_pebs ()
drm / i915 : Prevent using uninitialized MMIO funcs
[ ALSA ] soc - Clean up tabs
drm / i915 : Missed lock change with rps lock
proc : prevent stacking filesystems on top
ipmi : fasync BKL pushdown
USB : core : harden cdc_parse_cdc_header
clk : tegra : cclk_lp has a pllx / 2 divider
soc : ti : knav : Add a NULL pointer check for kdev in knav_pool_create
iio : st_pressure : Export sampling frequency for lps25h and lps331ap
drm : fix uninitialized acquire_ctx fields ( v2 )
rpmsg : glink : The mbox client knows_txdone
ibmvnic : Free tx / rx scrq pointer array when releasing sub - crqs
sfc : initialize dynamic sysfs attributes for lockdep
mtd : mxc_nand : Propagate the error if platform_get_irq () fails
ipvs : add check in ftp for initialized core
bonding : disable netpoll on enslave failure
[ media ] media : platform : s3c - camif : Constify platform_device_id
i40e : Fix memory leak at failure path in i40e_dbg_command_write ()
x86 , e820 : Guard against array overflowed in __e820_add_region ()
perf report : Compare symbol name for inlined frames when sorting
svcrdma : Add put of connection ESTABLISHED reference in rdma_cma_handler
uprobes : Allocate -> utask before handler_chain () for tracing handlers
ath9k : Do not use local_bh_disable in ampdu_action
virtio_net : enable big packets for large MTU values
blkcg : fix double free of new_blkg in blkcg_init_queue
drm / msm / hdmi : redefinitions of macros not required
[ ARM ] fix leak in iop13xx / pci
cyclades : fix read buffer overflow
[ DLM ] Fix memory leak in dlm_add_member () when dlm_node_weight () returns less than zero
iommu / vt - d : Fix a potential memory leak
USB : cdc - acm tell tty layer not to split things up .
vhost : actually track log eventfd file
[ SPARC ]: Add missing of_node_put
KVM : arm / arm64 : vgic : Fix kvm_vgic_map_is_active ' s dist check
KVM : emulate : avoid accessing NULL ctxt -> memopp
Staging : rtl8188eu : usb_halinit : Use ! x instead of x == NULL .
[ TCP ]: Fix TCP header misalignment
mtd : lpc32xx_slc : Make probe () return - EPROBE_DEFER if necessary
mtd : spear_smi : add NULL check on devm_kzalloc () return value
lib / decompress : set the compressor name to NULL on error
fscrypt : fix renaming and linking special files
staging : fbtft : Fix potential null dereference
[ SCTP ]: Fix kernel panic while received AUTH chunk while enabled auth
embedded : fix vc_translate operator precedence
s390 / qeth : use QDIO_ * _QFMT defines
perf sched : Look up thread using tid instead of pid
greybus : hd : fix cport - count check
sunrpc : integer underflow in rsc_parse ()
x86 / mm : Fix use - after - free of ldt_struct
iwlwifi : dvm : disable power save by default
selinux : fix kernel BUG on prlimit (..., NULL , NULL )
drm / i915 / sdvo : Cleanup connector on error path
libceph : delete from need_resend_linger before check_linger_pool_dne ()
USB : option : make interface blacklist work again
rpmsg : virtio : Fix possible double free in rpmsg_probe ()
cfg80211 : Clear " beacon_found " on regulatory restore
x86 / apic / irq_remap : Silence a bogus pr_err ()
Staging : ced1401 : Fixes trailing whitespace checkpath warning .
netrom : Fix use - after - free caused by accept on already connected socket
mac80211 : fix register_hw error path
drm / i915 : Change WARN to ERROR in CSB count
kcm : fix a null pointer dereference in kcm_sendmsg ()
[ PATCH ] ia64 : add ptr_to_compat ()
greybus : svc : Pass the correct pointer to input_free_device ()
staging : fbtft : make const array gamma_par_mask static
[ media ] ttusb - dec : buffer overflow in ioctl
xfs : fix quotacheck dquot id overflow infinite loop
ipmi : fix use - after - free of user -> release_barrier . rda
iwlwifi : reduce max remain - on - channel duration
ASoC : rockchip : Enable 192khz in hw_params operation
USB : cdc - acm : more sanity checking
iommu / amd : Fix BUG when faulting a PROT_NONE VMA
ALSA : usb - audio : Fix out - of - bound error
jiffies : add utility function to calculate delta in ms
ASoC : Fix reg_word_size for ak4104
rtc : ds1374 : Set . of_match_table to OF device ID table
iio : core : Handle error when mask type is not separate
drm / i915 : Fix CSR MMIO address check
[ media ] lirc : return ENOTTY when device does support ioctl
i40iw : Fix double free of allocated_buffer
nvmet - rdma : occasionally flush ongoing controller teardown
ipv6 - multicast : Fix memory leak in IPv6 multicast .
clk : hisilicon : Use kcalloc () in hisi_clk_init ()
USB : serial : console : fix use - after - free after failed setup
crypto : kpp - add get / set_flags helpers
[ XFS ] Fix realtime subvolume expansion , a porting bug b0rked it . Coverity
atm : ambassador : use NULL instead of 0 for pointer
pty : fix data loss when stopped (^ S /^ Q )
ipvs : allow rescheduling after RST
staging : ks7010 : change local variable type in ks_wlan_hw_rx
s390 / ipl : always use load normal for CCW - type re - IPL
virtio : fix memory leak in virtqueue_add ()
drbd : fix possible access after free
staging : unisys : delete extra space character in vbuschannel_itoa ()
ipv6 : ipv6_find_hdr restore prev functionality
drivers : thermal : parent virtual hwmon with thermal zone
thinkpad_acpi : Fix a memory leak during module exit
mac80211 : reset CQM history upon reconfiguration
drm / radeon : do not hard reset GPU while freezing on r600 / r700 family
NFS : Clean up : revert increase in READDIR RPC buffer max size
drivers / net / wireless / b43 / main . c : 4351 : Fixed coding style
reiserfs : Don ' t call reiserfs_get_acl () with the reiserfs lock
HID : hidraw , fix a NULL pointer dereference in hidraw_ioctl
mmc : sdhci - spear : Handle return value of platform_get_irq
mlxsw : spectrum : Fix ageing time value
mac80211 : fix conn_mon_timer running after disassociate
kernel / relay . c : fix potential memory leak
regmap : Silence warning on invalid zero length read
ASoC : tlv320aic3x : Don ' t sync first two registers from register cache
mac80211 : fix start_seq_num in Rx reorder offload
s390 / qeth : fix thinko in IPv4 multicast address tracking
ALSA : control : Make sure that id -> index does not overflow
ibmvnic : fix missing unlock on error in __ibmvnic_reset ()
ALSA : hda - Cirrus : Correctly clear line_out_pins when moving to speaker
bdi : register sysfs bdi device only once per queue
dmaengine : pl330 : do not emit loop for 1 byte transfer .
x86 - 64 : Fix the failure case in copy_user_handle_tail ()
iio : iio_buffer_register : Skip channels with negative scan index
ata_piix : add short 40c quirk for Acer Aspire 2030 , take # 2
drm / i915 : Hold rcu_read_lock when iterating over the radixtree ( vma idr )
tg3 : Fix 5906 link problems
malidp : Fix NULL vs IS_ERR () checking
usb : mon : make mmapped memory read only
[ SCSI ] scsi_lib : use correct DMA device in __scsi_alloc_queue
ext4 : Eliminate potential double free on error path
imx - drm : imx - drm - core : improve safety of imx_drm_add_crtc ()
arm64 : make sys_call_table const
HID : fix error condition propagation in hid - sony driver
ipv6 : udp : fix the wrong headroom check
staging / dgnc : fix info leak in ioctl
[ ALSA ] sound / isa / gus / interwave . c : check kmalloc () return value
UBI : mtd / ubi / vtbl . c : fix memory leak
batman - adv : randomize initial seqno to avoid collision
misc : fastrpc : prevent memory leak in fastrpc_dma_buf_attach
[ IPV6 ] ROUTE : Prefer reachable nexthop only if the caller requests .
dmaengine : zynqmp_dma : Fix race condition in the probe
Btrfs : fix extent_same allowing destination offset beyond i_size
scsi : libiscsi : Fix indentation
e1000 : check on netif_running () before calling e1000_up ()
staging : iio : lpc32xx_adc : Fix IRQ check
aacraid : missing capable () check in compat ioctl
tcp : fix potential memory corruption
i40e : Setting queue count to 1 using ethtool is valid
sched / migration : Move calc_load_migrate () into CPU_DYING
perf tools : Don ' t free list head in parse_events__free_terms
ARM : EXYNOS : Fix potential NULL pointer access in exynos_sys_powerdown_conf
mm : fix negative nr_isolated counts
ext4 : Fix overflow caused by missing cast in ext4_fallocate ()
staging : omapdrm / omap_gem_dmabuf . c : fix memory leakage
cifs : fix potential overflow in cifs_compose_mount_options
ath9k : do not register LEDs on AR913x
i2c : ocores : Fix pointer to integer cast warning
udp : fix if statement in SIOCINQ ioctl
perf intel - pt : Fix off - by - one comparison on maximum code
USB : prevent autosuspend during hub initialization
[ MTD ] [ NAND ] Casting bug in nand_default_block_markbad
[ POWERPC ] kprobes : Fix possible system crash during out - of - line single - stepping
[ media ] media / vivid - osd : fix info leak in ioctl
iio : adc : meson - saradc : unlock on error in meson_sar_adc_lock ()
keys : fix race with concurrent install_user_keyrings ()
[ TCP ]: Avoid a divide in tcp_mtu_probing ()
HID : emsff : properly handle emsff_init failure
partitions / msdos : FreeBSD UFS2 file systems are not recognized
isofs : avoid info leak on export
uwb : adds missing error handling
mwifiex : pcie : Fix memory leak in mwifiex_pcie_init_evt_ring
tun : handle register_netdevice () failures properly
Btrfs : Check for NULL page in extent_range_uptodate
x86 / fpu / xstate : Return NULL for disabled xstate component address
[ PATCH ] kprobes / ia64 : refuse inserting kprobe on slot 1
ceph : avoid divide by zero in __validate_layout ()
xen : make xen_flush_tlb_all () static
UBI : fix error path in __wl_get_peb
ARM : OMAP2 : Fix tusb6010 init error and compilation warning
gpio - 74x164 : Fix buffer allocation size
iwlwifi : don ' t ack the card state notification
dmaengine : zxdma : fix memset call
isdn : off by one in connect_res ()
ath10k : fix memory leak
ks8851 : Fix request_irq / free_irq mismatch
TOMOYO : Fix incomplete read after seek .
f2fs : wake up all waiters in f2fs_submit_discard_endio
USB : wusb : check CHID is all zeros before stopping the host
perf annotate browser : Exit when pressing ESC or the left arrow
ring - buffer : check for less than two in size allocation
ibmvnic : Sanitize entire SCRQ buffer on reset
drm / exynos : fix memory leak to EDID block
qlcnic : Dump mailbox command data when a command times out
drm / radeon / kms : prefer high post dividers in legacy pll algo
tcp : fix zero cwnd in tcp_cwnd_reduction
assoc_array : don ' t call compare_object () on a node
rtlwifi : rtl8188ee : Fix potential race condition
l2tp : fix info leak in l2tp_ip6_recvmsg ()
[ TCP ]: Fix sysctl_tcp_low_latency
[ PATCH ] off - by - 1 in kernel / power / main . c
[ media ] soc_camera : ret is never used . get rid of it
md : bcache : io . c : fix a potential NULL pointer dereference
[ ALSA ] Add missing compat ioctls for ALSA control API
ARM : davinci : neuros - osd2 : specify the chipselect in davinci_nand_pdata
Btrfs : send correct rdev and mode in btrfs - send
iio : chemical : ccs811 : Corrected firmware boot / application mode transition
USB : devio : fix issue with log flooding
staging : comedi : ni_660x : remove BUG_ON () in ni_660x_request_mite_channel ()
cxlflash : Fix to prevent stale AFU RRQ
dmaengine : at_hdmac : use tasklet_kill in teardown
[ media ] omap3isp : Fix buffer flags handling when querying buffer
thermal : rockchip : optimize sensor auto accessing period
scsi : qla2xxx : correctly shift host byte
ipv6 : ignore null_entry in inet6_rtm_getroute () too
rtlwifi : rtl8192se : Synchronize IRQ after disabling it
[ media ] videobuf2 : fix sparse warning
fuse : don ' t wake up reserved req in fuse_conn_kill ()
offb : the pseudo_palette is only 16 elements long
[ S390 ] cio : Fix ungroup vs . machine check race in ccwgroup .
ext3 : fdatasync should skip metadata writeout when overwriting
drm / amdgpu : update rev id for hainan
xfs : check for a valid error_tag in errortag_add
ath6kl : Check sme state before delivering disconnect event to cfg80211
[ PATCH ] USB : Resource leak fix for whiteheat driver
[ MIPS ] Sibyte : Fix name of clocksource .
powerpc / xive : Fix IPI reset
bcma : support chipsets with PMU and GCI cores ( devices )
KVM : fix LAPIC timer period overflow
Drivers : hv : fcopy : restore correct transfer length
staging : lustre : check for integer overflow
cpufreq : Avoid calling cpufreq driver ' s target () routine if target_freq == policy -> cur
[ SPARC64 ]: Allocate TSB node - local .
[ PATCH ] libata : fast exit from EH while unloading
ACPI : resolve HP nx6125 S3 immediate wakeup regression
ext2 : Check return value from ext2_get_group_desc ()
pppoe : Lacks DST MAC address check
virtio : console : Make write () return - ENODEV on hot - unplug
usb - uas : set max_lun and max_channel
mmc : sdhci - acpi : Handle return value of platform_get_irq
mips : handle pgtable_page_ctor () fail
dlm : avoid unnecessary search in search_rsb
ASoC : blackfin : Fix missing break
mac80211 : fix statistics leak if dev_alloc_name () fails
usb : gadget : atmel_usba_udc : Remove unneeded condition
xfs : fix off - by - one error in xfs_attr3_rmt_verify
ixgbe : add check for VETO bit when configuring link for KR
md : use kzalloc () when bitmap is disabled
console / vgacon . c : mark file - local symbol static
[ media ] bfin_capture : fix compiler warning
blkcg : don ' t create " io . stat " on the root cgroup
i7core_edac : don ' t free on success
hwmon : ( adm1025 ) Fix vrm write operation
blkcg : stop iteration early if root_rl is the only request list
tg3 : Restrict ASPM workaround devlist
perf thread_map : Correctly size buffer used with dirent -> dt_name
perf , x86 : Fix Intel fixed counters base initialization
tun : allow positive return values on dev_get_valid_name () call
module : fix memory leak on early load_module () failures
irqchip / gic - v3 : Fix out - of - bound access in gic_set_affinity
libbpf : fix string comparison for guessing eBPF program type
selinux : fix error return code in policydb_read ()
rtc : limit frequency
HSI : nokia - modem : Use devm_kcalloc () in nokia_modem_gpio_probe ()
[ PATCH ] powerpc : IOMMU : don ' t ioremap null addresses
ip_gre : Set needed_headroom dynamically again
mac80211 : minstrel : avoid port control frames for sampling
ASoC : wm5102 : Correct lookup of arizona struct in SYSCLK event
drm / i915 : check the power domains in ironlake_get_pipe_config ()
isdn / gigaset : adjust usb_gigaset tty write buffer limit
[ ALSA ] sound / pci / cs46xx /: fix an off - by - one
ext4 : remove unnecessary ext4_get_group_info in ext4_mb_load_buddy
rt2x00 : Use IEEE80211_TX_CTL_STBC flag
crypto : qat - silence a static checker warning
mac80211 : Apply ht_opmode changes in ieee80211_change_bss
PCI : cpqhp : register busn_res
tty : amba - pl011 : don ' t dereference NULL platform data
iommu / rockchip : Play nice in multi - platform builds
i40e : Expose ' trust ' flag to userspace via ndo_get_vf_config .
[ CIFS ] Possible null ptr deref in SMB2_tcon
iio : adc : xilinx - xadc : assign auxiliary channels address correctly
x86 : check range in reserve_early ()
dma : struct device - replace bus_id with dev_name (), dev_set_name ()
ipv6 : fix a typo in fib6_rule_lookup ()
af_key : initialize satype in key_notify_policy_flush ()
[ media ] marvell - ccic : fix V4L2_PIX_FMT_SBGGR8 support
sched : Fix information leak in sys_sched_getattr ()
drivers / video / backlight / lcd . c : call put_device if device_register fails
block : fix use - after - free in seq file
[ TCP ]: Do receiver - side SWS avoidance for rcvbuf < MSS .
drm / exynos : add pid to g2d_runqueue_node
sctp : fix possible seqlock seadlock in sctp_packet_transmit ()
drm / i915 : Can ' t do accurate vblank timestamps with UMS
[ media ] drivers / media / pci / saa7134 / saa7134 - dvb . c : Test if videobuf_dvb_get_frontend return NULL
video : vt8500lcdfb : Remove kfree call since devm_kzalloc () is used
perf ui hist browser : Fix segfault on ' a ' for annotate
liquidio : update module parameter fw_type to reflect firmware type loaded
mnt : Fix the error check in __detach_mounts
KVM : nVMX : No need for handle_vmx_insn function any more
e1000e : e1000e_cyclecounter_read (): fix er32 ( SYSTIML ) overflow check
vmwgfx : return an - EFAULT if copy_to_user () fails
ipmi : fix platform return check
[ ALSA ] fix resource leak in usbmixer
KVM : x86 : emulator : Return to user - mode on L1 CPL = 0 emulation failure
ixgb : convert dev -> priv to netdev_priv ( dev ).
i40evf : Add additional check for reset
cxl : remove redundant increment of hwirq
clk : shmobile : Free ' clock ' on error path
chipidea : error on overflow for port_test_write
cs5535 - mfgpt : don ' t call __init function from __devinit
ISCSI : fix minor memory leak
Btrfs : fix inode eviction infinite loop after extent_same ioctl
mfd : Use the AB8500 ' s IRQ domain to convert hwirq to virq
usb : phy : r - car gen2 : use usb_add_phy_dev
bpf : Fix integer overflow in prealloc_elems_and_freelist ()
f2fs : exclude special cases for f2fs_move_file_range
[ media ] vivid : fix broken Bayer text rendering
drm / nouveau / core : swap the order of imem / fb
RDS : null pointer dereference in rds_atomic_free_op
ARM : 8305 / 1 : DMA : Fix kzalloc flags in __iommu_alloc_buffer ()
[ CPUFREQ ] Do not set policy for offline cpus
Btrfs : add missing error check to find_parent_nodes
acer - wmi : avoid the warning of ' devices ' may be used uninitialized
[ XFRM ]: Use output device disable_xfrm for forwarded packets
igb : Bump version to 3 . 0 . 6
drm / radeon / kms : fix macbookpro connector quirk
futex_unlock_pi () hurts my brain and may cause application deadlock
dm : mark function lists static
ocfs2 : fix a memory leak in __ocfs2_move_extents ()
USB : serial : io_ti : fix div - by - zero in set_termios
ath9k : check for NULL rate when using ieee80211_get_rts_cts_rate ()
ceph : avoid null deref in osd request error path
[ media ] v4l2 - mem2mem : clear m2m queue ready counter in v4l2_m2m_streamoff
acer - wmi : set the touchpad toggle key code to KEY_TOUCHPAD_TOGGLE
ASoC : rt5514 : Fix uninitialized calibration value
Input : ALPS - fix command mode check
bnx2x : Add BCM84823 to the supported PHYs
xen / pci : Check for PCI bridge before using it .
ath9k : fix channel frequency calculation for AR9340
iomap : prepare iomap_fiemap for attribute mappings
ARM : mxc : free dma_mask in error path
KVM : MMU : Set shadow pte atomically in mmu_pte_write_zap_pte ()
cpufreq : intel_pstate : Fix limits -> max_perf rounding error
USB : serial : visor : fix crash on detecting device without write_urbs
eCryptfs : Copy up lower inode attrs after setting lower xattr
mmc : atmel - mci : r / w proof capability only available since v2xx
i2c : core : smbus : fix a potential missing - check bug
kbuild : fixdep : Check fstat ( 2 ) return value
stmmac : re - add IFF_UNICAST_FLT for dwmac1000
x86 / msr : Add capabilities check
KVM : x86 : Check lapic_in_kernel () before attempting to set a SynIC irq
arm64 : Move BP hardening to check_and_switch_context
dmaengine : fix dependency chaining
drm / vmwgfx : Don ' t flush fb if we ' re in the suspended state .
staging : r8723au : move constant to right of comparison test
USB : serial : io_ti : fix information leak in completion handler
xhci : Set URB actual length for stopped control transfers
iio : buffer : Fix NULL pointer deref caused by empty scan mask
imx - drm : ipu - dp : Check the return value of devm_kzalloc ()
kconfig : Avoid buffer underrun in choice input
ext4 : Avoid null pointer dereference when decoding EROFS w / o a journal
ASoC : intel : log an error on double free
genetlink : Fix a memory leak on error path
mwifiex : fix command 0x2c timeout during p2p_find or p2p_connect
HID : elo : fix memory leak in elo_probe
mfd : db8500 - prcmu : Check return of devm_ioremap for error
drm / nouveau / i2c / gp104 : initial support
sg_start_req (): make sure that there ' s not too many elements in iovec
[ media ] em28xx : Fix vidioc fmt vid cap v4l2 compliance
drm / amdkfd : Handle case of invalid queue type
aacraid : prevent invalid pointer dereference
ath6kl : unblock fwlog_block_read () on exit
sock_diag : off by one checks
mtd : mtdpart : fix add_mtd_partitions error path
audit : don ' t generate audit feature changed log when audit disabled
watchdog : bcm7038 : Check the return value from clk_prepare_enable ()
ASoC : omap - pcm : Free dma buffers in case of error .
af_key : initialize xfrm encap_oa
rtlwifi : btcoex : 21a 2ant : just return when wifi is under ips
mac80211 : fix scan_sdata assignment
m68k : macide doesnt check for Mac
dm crypt : use memzero_explicit for on - stack buffer
gma500 : Consider CRTC initially active .
staging : ozwpan : Set farewell report length .
avr32 : Fix sigaltstack behaviour
x86 , mce : reinitialize per cpu features on resume
vfs : ioctl : prevent double - fetch in dedupe ioctl
KEYS : Fix keyring ref leak in join_session_keyring ()
drm / i915 : switch off backlight for backlight class 0 brightness
x86 / boot : Fix minor fd leakage in tools / relocs . c
[ SCSI ] scsi_transport_iscsi : make priv_sess file writeable only by root
drm / helper : lock all around force mode restore
ASoC : simple - card : tidyup return method from probe ()
USB : PD : define specific requests
hpsa : Checking for a NULL return from a kzalloc call
memcg : hierarchy avoid unnecessary reclaim
[ NETFILTER ]: recent match : missing refcnt initialization
[ XFS ] Wait for all I / O on truncate to zero file size
greybus : svc : disable connection after all its users are gone
dmaengine : pxa_dma : declare transfer are reusable
mmc : debugfs : Print ext_csd in ascending order
mfd : tc6393xb : Handle return value of clk_prepare_enable
ixgbe : return error on unsupported SFP module when resetting
usb : chipidea : add system interface for ttctrl . ttha
cdrom : information leak in cdrom_ioctl_media_changed ()
staging : lustre : libcfs : fix aligment issue
Btrfs : free and unlock our path before btrfs_free_and_pin_reserved_extent ()
drm : Fixup locking WARNINGs in drm_mode_config_reset
drm / mediatek : plane : Use FB ' s format ' s cpp to compute x offset
batman - adv : Calculate extra tail size based on queued fragments
bq20z75 : Enable detection after registering
drm / nouveau / pmu : make sure the reset hook exists before running it
serial : 8250 : of : Defer probe on missing IRQ
ARM : mm : allow set_memory_ *() to be used on the vmalloc region
iwlwifi : protect SRAM debugfs
staging : dgnc : Use kcalloc instead of kzalloc .
staging , vt6656 / wpactl . c : Fix mem leak in wpa_ioctl ()
iwlagn : block regular host commands if driver don ' t own uCode
[ MTD ] CORE mtdchar . c : fix off - by - one error in lseek ()
ALSA : compress : fix an integer overflow check
drm / mgag200 : free memory allocated with alloc_apertures ()
OMAPDSS : TPO - TD03MTEA1 : Check for errors from regulator_enable ()
ath5k : temporarily disable crypto for AP mode
regmap : cache : Don ' t attempt to sync non - writeable registers
mm : enforce min addr even if capable () in expand_downwards ()
drm / radeon : add backlight quirk for hybrid mac
inotify : fix coalesce duplicate events into a single event in special case
s390 / dasd : fix list corruption for sleep_on requests
iio : accel : bmc150 - accel : fix counting direction
esas2r : fix an error path in esas2r_ioctl_handler
cifs : always do is_path_accessible check in cifs_mount
ALSA : seq : Fix race at timer setup and close
ipv4 : Missing sk_nulls_node_init () in ping_unhash ().
iwlwifi : mvm : Set valid TX antennas value before calib request
tracing / filters : free pred when clearing filters
iio : imu : adis16400 : fix memory leak
ipvs : fix info leak in getsockopt ( IP_VS_SO_GET_TIMEOUT )
clk : shmobile : r8a7795 : Add USB3 . 0 clocks
tracing : Add entry -> next_cpu to trace_ctxwake_bin ()
mm : avoid endless looping for oom killed tasks
gpio : Propagate all errors in devm_get_gpiod_from_child ()
[ SCSI ] bnx2i : Added reconnect fix connecting against Lefthand targets
drm / i915 : Don ' t leave old junk in ilk active watermarks on readout
brcmfmac : Fix memory leak and missing assignment .
kvm : nVMX : Don ' t allow L2 to access the hardware CR8
lockd : fix regression in lockd ' s handling of blocked locks
ipv4 : dont drop packet in defrag but consume it
mtd : nand : ifc : Initialize SRAM for all version >= 1 . 0
platform / x86 : dell - wmi : check for kmalloc () errors
drm / radeon : fix typo in finding PLL params
KEYS : close race between key lookup and freeing
ASoC : da8xx / omap - l1xx : match codec_name with i2c ids
pinctrl : sh - pfc : add missing of_node_put
mlxsw : spectrum_router : Compare only trees which are in use during tree get
blk - mq : Wake tasks entering queue on dying
mmc : mxcmmc : handle mmc_of_parse () errors during probe
Btrfs : fix an oops when deleting snapshots
mac80211 : Keep skb -> piority for relayed frames in AP mode
ARM : at91 / gpio : fix display of number of irq setuped
[ media ] sony - btf - mpx : Drop needless newline in param description
Staging : bcm : info leak in ioctl
mmc : Fix mmc_delay () function
atmel - mci : Fix memory leak in atmci_regs_show
powerpc / 64 : Don ' t use early_cpu_has_feature () in cpu_ready_for_interrupts ()
[ media ] cec : ignore messages when log_addr_mask == 0
tools : iio : iio_utils : check amount of matches
[ IA64 ] kexec fails on systems with blocks of uncached memory
mac80211 : fix channel type recalculation with HT and non - HT interfaces
powerpc / pseries : Optimize IOMMU setup
[ PATCH ] ipw2200 : mark " iwconfig retry 255 " as invalid
block : fix use - after - free in sys_ioprio_get ()
HID : hyperv : match wait_for_completion_timeout return type
jbd2 : fix use after free in kjournald2 ()
ALSA : hda - Enable widget power saving for Realtek codecs
lock debug : sit tight when we are already in a panic
make sure do_dentry_open () won ' t return positive as an error
bpf : force strict alignment checks for stack pointers
crypto : user - fix memory leak in crypto_reportstat
scsi : storvsc : fix SRB_STATUS_ABORTED handling
l2tp : fix info leak via getsockname ()
crypto : drbg - fix semicolon . cocci warnings
[ SCSI ] zfcp : Wait for port scan to complete when setting adapter online
iommu / ipmmu - vmsa : IPMMU device is 40 - bit bus master
cris : add missing declaration of kgdb_init () and breakpoint ()
ALSA : bebob : Fix a missing to unlock mutex in error handling case
drm / amd / display : Check msg -> size before starting aux transfer
sfc : check for allocation failure
block : Fix io_context leak after clone with CLONE_IO
dm crypt : missing kfree in ctr error path
[ media ] V4L2 : mt9t112 : fixup JPEG initialization workaround
[ PATCH ] ext2 : prevent div - by - zero on corrupted fs
mwifiex : fix leak of gen_ie storage on exit from mwifiex_del_mgmt_ies
ALSA : seq : Cancel pending autoload work at unbinding device
ovl : ignore permissions on underlying lookup
[ PATCH ] cpufreq : fix section mismatch warnings
ALSA : usb - audio : Fix NULL dereference in create_fixed_stream_quirk ()
iwlwifi : Fix memory leak in iwl_req_fw_callback ()
bnx2x : Calling pci_set_drvdata earlier
drm / i915 : Return stored value from max freq sysfs entry
prlimit : do_prlimit needs to have a speculation check
[ ALSA ] seq_midi_event : fix parsing of F9 / FD bytes
clk : imx7d : Correct ahb clk parent select
fs : Rearrange inode structure elements to avoid waste due to padding
Btrfs : add missing kfree in btrfs_destroy_workqueue
seq_buf : Fix overflow in seq_buf_putmem_hex ()
bpf : fix liveness propagation to parent in spilled stack slots
rds : Fix NULL pointer dereference in __rds_rdma_map
tproxy : allow non - local binds of IPv6 sockets if IP_TRANSPARENT is enabled
tcp : initialize rcv_mss to TCP_MIN_MSS instead of 0
tracepoints : use rcu_ * _sched_notrace
dm kcopyd : avoid pointless job splitting
ath10k : ensure txrx - compl - task is stopped when cleaning htt - tx
drivers / video / igafb . c : introduce lost ' return '
[ MTD ] [ OneNAND ] fix call to onenand_verify when writing subpages
powerpc : Check for valid hugepage size in hugetlb_get_unmapped_area
[ media ] v4l : vb2 - memops : use vma slab when vma allocation
NFSv4 : Make sure unlock is really an unlock when cancelling a lock
yam : fix a memory leak in yam_siocdevprivate ()
ARM : mxc : platform - mxc - mmc : Fix register region size
powerpc / bpf : Use memset32 () to pre - fill traps in BPF page ( s )
vfs : make gcc generate more obvious code for acl permission checking
[ media ] r820t : Fix IF scale
f2fs : turn down IO priority of discard from background
nvmet : fix space padding in serial number
scanner : don ' t crash on strings containing a NUL byte
fix in thread . call
add sanity check for reading multipart files with no parts (# 840 )
reduce B44 _tmpBufferSize ( was allocating two bytes per byte ) (# 843 )
Enforce a limit on value bytes size
add assert for addition overflow on queue creation (# 225 )
Protected against garbage in the CapRTL encoder
Added stack overflow check for hermes :: vm :: hermesBuiltinApply
eliminated double - free in the parsing of dist points
libnsgif : Fix null pointer deref on frameless GIF input
libnsgif : fix oob in lzw_decode
mm : fix prctl_set_vma_anon_name
fix heap - buffer - overflow in mp4read . c
zero the arep structure ( Coverity # 115822 )
lib / bidi : Fix integer overflow ( found by fuzzing )
encode : fix empty FIELD_2DD_VECTOR
Fixed potential buffer overflow in ` ProcessRadioRxDone `
Fix a number - up crash bug ( Issue # 413 )
fixed missing return status test error
secvar : fix endian conversion
stream : fix false negative on bad RST
Fixed typo while calculating module path length .
Fixed frame allocation from an awaited frame .
cJSON : fix memory leak on object parsing error
[ core ] fix parse_via () parsing
[ commissioner ] add network name length check in GeneratePskc () (# 4404 )
KML : set OAMS_TRADITIONAL_GIS_ORDER for SRS returned on returned layers
Decoder : Fixed error handling for dangling fields
Decoder : Set u1_long_term_reference_flag to 0 for error concealment
Decoder : Fix for handling invalid intra mode
Decoder : Initialize default reference buffers for all pictures
IGraphicBufferProducer : fix QUEUE_BUFFER info leak
IGraphicBufferConsumer : fix ATTACH_BUFFER info leak
Fix a wrong check in rfc_parse_data
fixed segfault in GIF reading ; this fixes https :// github . com / pts / sam2p / issues / 67
GenericSource : reset mDrmManagerClient when mDataSource is cleared .
Resolve a merge issue between lmp and lmp - mr1 +
IOMX : do not clear buffer if it ' s allocated by component
modbus_reply : fix copy & paste error in sanity check ( fixes # 614 )
misc : update : fix buffer overflow in updater
codec : schroedinger : fix potential buffer overflow .
avoid XSS vulnerability when using OIDCPreservePost On
fixed overflow vulnerability
resolve BUGFIX handle yet unresolved if - features
Prevent a CHECK - fail due to empty tensor input in ` map_stage_op . cc `
Fix a shape inference issue leading to nullptr deref .
Prevent one more div by 0 in TFLite
Prevent array write out - of - bounds .
[ lite ] Add validation check for dilation height / width to be positive integers .
Prevent a division by 0
Prevent dereferencing of null pointers in TFLite ' s ` add . cc `.
Fix the CHECK failure in tf . raw_ops . QuantizeAndDequantizeV2 .
Remove unused out - of - bounds memory access from MTM loader (# 249 )
[ tls ] Treat invalid block padding as zero length padding
transport / server . cc : Return correct size of decompressed lz4 buffer
bugfix (# 245 ) - long incoming message crashes server
ksmbd : check nt_len to be at least CIFS_ENCPWD_SIZE in ksmbd_decode_ntlmssp_auth_blob
remove assert that can reasonably be expected to happen
ensure fragment offset is less than total length
store mode : check for truncation
Fix the | name | of PerformanceNavigationTiming
Avoid an overflow in harfbuzz - myanmar . c
chrome_repack_locales : compress . pak files on Chrome OS
[ scheduler ] Remove implicit fallthrough in switch
gpu : Disallow use of IOSurfaces for half - float format with swiftshader .
android : Stop child process in GetTerminationInfo
Drive out additional flakiness of TouchAction browser test .
Fix a lock re - entry bug in GpuDataManager :: UpdateGpuInfo .
Removing unnecessary DCHECK from SafeBrowsing interstitial .
Add a check to prevent len from going negative in xmlParseAttValueComplex .
return early from ProcessRawBytesWithSeperators () when data length is 0
Hide the " Open PDF in Reader " bubble on navigations .
[ MediaStream ] Pass request ID parameters in the right order for OpenDevice ()
Disables PanelBrowserTest . MinimizeTwoPanelsWithoutTabbedWindow on
omnibox : Remove unregistered NOTIFICATION_BOOKMARK_MODEL_LOADED from action_box test .
Prevent regular mode session startup pref type turning to default .
Add CHECKs for file descriptors used in select () by InotifyReaderTask
[ DevTools ] Use no - referrer for DevTools links
Handle a bad XSLT expression better .
Tests were marked as Flaky .
Remove an unused variable .
fix incorrect TexImage3D params w / UNPACK_IMAGE_HEIGHT
Fix a crash in HTMLFormElement :: prepareForSubmission .
rm flag and rebaseline
ash : Fix right - alignment of power - status text .
Take default system font size from PlatformFont