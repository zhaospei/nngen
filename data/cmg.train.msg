siddec : reset bytes counter
mpeg2dec : Fix buffer memory leak in error code path
mpeg2dec : Hold ref to buffer while mapped
realmedia : Fix Memory leak in error case
rdtdepay : make buffer metadata writable before setting caps on it
diff : avoid potential null pointer dereference on error
df : Skip final '/' character when calculating basename .
fish : Fix memory leak in inspect /- i code ( detected by valgrind ).
modbus plugin : fix 2 leaks on error paths
write_sensu plugin : malloc + memset -> calloc
bind plugin : plug small leak
perl plugin : Warn if " EnableDebugger " has been used after " LoadPlugin ".
src / utils_avltree . c : Add sanity check to c_avl_destroy ().
cpu plugin : Collectd CPU ` steal time ' as reported by new Linux kernels .
teamspeak2 : malloc + memset -> calloc
Turbostat : Use the ' count ' gauge to count SMIs
write_http plugin : Add assertion for scan - build .
postgresql plugin : Disable queries that do not include an SQL query string .
utils_cache plugin : Fix memory leak .
* kdb . h ( krb5_def_promote_db ): Declare
Always stat profile files after reopening them
Print a " starting ..." message on stderr if running in nofork mode , for
don ' t forget to liberate etype
fall back on realm name as a default domain name
kadm5_setkey_principal_3 not copying key_data_ver and key_data_kvno
Reposition a trace call which was dereferencing freed memory after
src : remove cpplint error using NOLINT directive
buffer : guard against integer overflow
src : fix use of uninitialized variable
crypto : fix memory leak in ECDH :: SetPrivateKey
crypto : fix memory leak in randomBytes () error path
src : fix minor memleak in preload - modules
crypto : fix memory leak in CipherBase :: Final
cid # 10403 follow - up
fixed CID 11651
fix CID # 10540
cid 12207 - fix error state for allocating altstr container
show errno reason in freshclam on failure ( bb # 923 )
fix NULL dereference when running with a limited db
fix write () on a file desriptor of - 1 , bb # 5784
cid 12123 - fix error state in allocating memory in pdf finalize
High : cib : Fix the feature set check
Dev : crmd : fix typo
Put our CIB into slave mode on exit
Low : execd : avoid memory leak when testing remote key
TE : Detect changes to attributes of the cib object
Low : tools : crm_mon - resolve coverity UNINIT error
PE : Prevent a potential use of NULL
High : cib : Fix huge memory leak affecting heartbeat - based clusters
Low : controller : filter certain synthesized failures from CIB
Low : tools : crm_simulate - fix small xpath memory leak in inject_node_state ()
TE : Always exit via mainloop
High : cib : Correctly free memory when writing out the cib to disk
crmd : Provide useful feedback when the metadata is unreadable
High : PE : Fix memory leak on processing message ( bnc # 780224 )
cib : Memory leak - Free query matches in update_attr ()
Dev : Tools : attrd - Fix value ++ expansion
High : PE : Parse fail - count correctly ( it may be set to ININFITY )
gui : Show messages as text not html
RPC : strictly require HTTP URI "/"
util : handle NULL pointers passed to git_strarray_free ()
odb : mempack : fix leaking objects when freeing mempacks
fileops : do not overwrite correct error message on mmap
config : set an error message when asked to delete a non - existent key
remote : don ' t try to create tag annotations as refs / tags / v0 . 1 . 0 ^{}
tree : Check for `. git ` with case insensitivy
oid : handle NULL pointers passed to git_oid_shorten_free ()
fileops : fix invalid read
path : remove unused local variable
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
Added an extra check to MLU bounds
make_absolute_path : return the input path if it points to our buffer
Axe the last ent
blame : don ' t overflow time buffer
make -- find - copies - harder imply - C
pack - objects : fix early eviction for max depth delta objects
fix pack - object buffer size
range - diff : right - trim commit messages
grep : fix colouring of matches with zero length
send - pack : avoid deadlock on git :// push with failed pack - objects
git - add : warn when adding an ignored file with an explicit request .
refs : prevent accidental NULL dereference in write_pseudoref
git - log : do not output excess blank line between commits
rev - list -- header : output format fix
get_sha1_oneline : check return value of parse_object
setup . c : verify_non_filename (): don ' t die unnecessarily while disambiguating
xmkstemp (): avoid showing truncated template more carefully
setup : make sure git dir path is in a permanent buffer
wt - status . c : fix a memleak
http - push : do not SEGV after fetching a bad pack idx file
builtin - commit : fix summary output .
[ PATCH ] HTTP partial transfer support fix .
doveadm user : Avoid potential crashes when running via doveadm - server
fts : fts_expunge_log_uid_count () returned garbage on nonexistent log .
imap : Mark DELETE command as potentially closing mailbox .
auth : Fixed non - auth passdb lookup when password had ".< encoding >" suffix .
lib - master : Minor memory leak fix on deinit .
doveadm proxy : Don ' t crash if remote doesn ' t support log proxying
mail_modifylog_mark_synced () didn ' t update mmap
lib - smtp : server : Fix assertion failure occurring for a second failed BDAT / BURL command .
imapc : Don ' t send UID FETCH 1 :* after SELECT if mailbox is empty
lib - fs : ostream - metawrap shouldn ' t count metadata header as part of the stream offsets .
squat_trie_lookup (): Make sure we clear result arrays first .
dbox rebuild : If alt directory doesn ' t exist , don ' t fail .
lib - storage : Fix accessing the same " raw " mailbox multiple times in process .
lib - smtp : smtp_server_connection_pending_command_data () - don ' t crash if smtp_parser is NULL
fs layout : Absolute paths as mailbox names shouldn ' t return index with INDEX = MEMORY
pop3 - migration : Don ' t even try to match message sizes with pop3_migration_skip_size_check = yes
rfc822_parse_phrase (): Don ' t read outside data boundaries if input is empty .
pgsql : Avoid epoll_ctl () errors when closing connection .
doveadm backup : Don ' t unnecessarily delete mailbox when handling expunges
snarf : Make sure destination mailbox is open when syncing it .
Die if ( Solaris ) LDAP library returns wrong file descriptor .
imapc : Fix crash in mailbox_exists () when LAYOUT isn ' t imapc .
dict - file : Updated dotlock settings .
lib - ssl - iostream : Fixed a memory leak when verifying certificate name .
imap : APPEND crashed if invalid keyword was given as parameter .
lib : Add asserts to t_abspath and t_abspath_to
lib - storage : Fixed assert - crash caused by previous stats change .
sdbox : In - memory mailbox GUID differed from on - index GUID after mailbox_create ().
lib - index : Compiler warning fix
lib - auth : Don ' t crash in auth_master_ *() lookups if caller hasn ' t created ioloop .
i_getpwnam (): Ignore EINVAL errors silently .
lib - program - client : Fix memory access problem
i_stream_read (): Added assert to make sure EOF isn ' t returned too early .
fts : Fixed handling NOT when using lib - fts
lib - storage : Fixed hooks with plugins that didn ' t override any vfuncs
lib - imap - storage : Fixed leaking istream on error conditions .
fts : Fixed crash with some search queries in empty mailbox .
lib - index : Minor fix to handling atomic inc with INT_MIN value .
lib - index : mail - index - transaction - export : Fixed handling of transaction_flag_updates_have_non_internal () return value .
lib : io_loop_set_current () - do nothing if ioloop doesn ' t change
ssl - iostream : ssl_iostream_destroy ( NULL ) should be a no - op
DEBUG : buffer_verify_pool () crashed with empty buffer .
lib - storage : If chdir ( home ) fails due to EACCES , don ' t log two errors .
push - notification : Fix mailbox unsubscribe event
memset () the returned structure , just in case .
placeholder -> noselect change broke LIST "" " foo /%" not showing foo /.
libpassword : Deinitialize hash iterator in password_scheme_detect
lib - http : 32bit system fix .
acl : Crashfix for handling namespaces that don ' t have a paths .
imapc : Avoid duplicate FETCH BODY . PEEK [] when mail is already expunged
Removed accidentally committed debug warning .
reset modifylog sync mark at the end of opening index file
doveadm - save : Set exit code to EX_TEMPFAIL on open error
lib - storage : mailbox_list_delete_trash () - Don ' t ignore errors
authd : remove some whitespace
conf : Remove dead store .
libratbox / openssl : Fix possible memory leak with SSL certificate fingerprints
ldap : silence clang warning
file_range : avoid integer overflow when figuring out byte range
fix compiler warning : comparison between signed and unsigned
ConnectionExists : make sure conn -> data is set when " taking " a connection
openssl : fix boringssl build again
cookie : fix memory leak on oversized rejection
fix compiler warning
typecheck - gcc : allow CURLOPT_STDERR to be NULL too
Fixed another OOM problem , this time with test 64 .
mime : properly unbind mime structure in curl_mime_free ().
read_tcp_data () fix to get the proper buffer pointer and size
curl_ntlm_msgs . c : revert commit 463082bea4
sasl_sspi : Removed note about the NTLM functions being a wrapper
AddFormData : prevent only directories from being posted
sasl : Fixed a memory leak on OOM
config - amigaos . h : ( embarrassed ) made the line shorter
sftp upload : expire to advance state machine
geez , I ' d ruined Nico ' s socklen_t define , corrected it now
free the allocated mailbox name at disconnect
conncache : fixed memory leak on OOM ( torture tests )
Curl_pipeline_server_blacklisted : handle a NULL server name
setopt : fix CURLOPT_SSH_AUTH_TYPES option read
get the display host name properly
avoid redundant work when reusing same connection
mbedtls : fix memory leak in error path
avoid proxy ( and other ) auth if no user + password is given
winbind : Fixed ntlm_auth expecting eol following commit e17c1b25bc33eb
urlglob : avoid NULL pointer dereference
Bug 592484 - invalid write in HB
Prevent an assertion with invalid Unicode sequences
Fixed unit test for mom_job_func
Fixed unit test for pbsd_main .
Added header to momctl unit test to resolve do_mom
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
Freed up an allocated pointer prior to a return .
fixed unit test
undo partial patch
Fixed problem with torque HA where trqauthd was
pua : safety check on tuple_id len
introduced a new # include in response to Andrei ' s uri changes
pua_dialoginfo : do not free dlginfo_cell too early
modules / ims_qos : missing return statement
core : free SE header parsed structure
rtpproxy : use reallocxf () to handle realloc failure
ctl : free allocated pointer in case of error
tm : define tm_ctx_get () to NULL if no WITH_TM_CTX
mohqueue : fix pointer for RTP statistics error
kazoo : check amqp version
erlang : Fix memory leak
kex : check if uptime value was initialized
tm : Ensure all contact attributes are initialised to NULL when serial forking
app_python3 : skip execution of child_init () for rank PROC_INIT
db_postgres : don ' t free pg query structure in store result function
modules / usrloc : init _ul_sruid also in mi_child_init
pv : rpc commands updated to use rpl_printf ()
acc ( k ): Replace call to ( deprecated ) atoi () function with
carrierroute : export MI commands
* Fixed test on From URI length . Thanks to Papadopoulos Georgios for
tmx : allocate space to store ending 0 for branch value
Added some errorchecking in rs_set_warmth_auto ().
Fixed camera WB for Nikon D7100 .
Added some sanity checking in gui_drawingarea_motion_callback ().
Made paste defualt to all settings .
Changed a malloc to g_malloc
stream_id == MAX_STREAMS is invalid , since counting starts with 0 .
vo_opengl : do not leak previous FBO when reallocating it
fastmemcpy path fixed
Initialize frame rate extension fields in mp_header_process_sequence_header
af_scaletempo : Fix crash in option parsing
vo_opengl : disable drm_egl autopickup
vo_opengl : don ' t upload potentially uninitialized memory to GL buffer
input : fix off - by - one error
demux_mkv : fix minor memory leak
audio : fix small memory leak
vaapi : fix uninitialized value read
check sh_video before seeking ; closes cid 56
vo_opengl : reset surfaces on equalizer updates
vo_opengl : fix extension name
fix scrolling status line in windows
add YAXPAX and YAPAXI exports to msvcr80
workaround ! exit if there ' s no video stream ( hopefully it ' s a proper fix )
vo_opengl : error out gracefully when trying to use FBOs without FBO API
ad_spdif : Fix crash when spdif muxer is not available
gl_video : fix refcounting
ao_null : disable latency emulation
don ' t call video -> get_info if it ' s NULL
ao_wasapi : don ' t treat SetDisplayName () failure as fatal
Added the connect2Server prototype .
av_log : map libav * debug messages to MSGL_DBG2
fix null pointer check in add_subtitles ,
vo_direct3d : allow resizing before video init
vo_rpi : fix blackscreen before the first subtitle / OSD is rendered
vo_direct3d : fix bug in screenshot code with some pixel formats
af : avoid rebuilding filter chain in another minor case
demux_libass : actually set priv context
reset channels too
video_out : free memory if initializing a VO fails
mplayer : don ' t show bogus audio PTS during hr - seek
sanity check : since chapter is 0 - based it can ' t exceed nr_of_ptts - 1
vo_vaapi : fix reference counting when using software decoding
ao_coreaudio_utils : compare full AudioStreamBasicDescription
Bugfix : ztex : Free bitstream in memory when done with it
Bugfix : lowl - ftdi : ft232r_close : Free device handle
Add the cgpu_info structure before avalon reset .
Update the hashmeter at most 5 times per second .
Bugfix : setup_stratum_curl : Hold stratum lock until connection completes , to avoid potential races
Increase the speed of hashrate adaptation .
Bugfix : free adhoc string elist element when removing it from list
Remove off by one error .
Bugfix : hashbusterusb : Correct return value of hashbusterusb_vrm_unlock
Bugfix : Pass newlines as ASCII ( fixes lack of them in non - unicode builds )
Bugfix : Free stratum nonce1 before replacing it with new value on reconnect
Bugfix : Free shares discarded before beginning submission
Bugfix : knc : Perform le32toh on unknown - work nonces
Tables : Fix spelling mistake in comment
aapts : Update for ACPICA interface change .
ACPICA : Fix for Linux source translation
AcpiDump : Allow tables that contain only an ACPI table header .
automated code cleanup
Ran acpisrc source cleanup
iASL : Update for ToPLD macro .
scsi : Make device scsi - disk reject / dev / sg *
target - cris : Build fix for debug output
usb : queue can have async packets
target - arm : fix unsigned 64 bit right shifts .
xilinx_spips : Fix bus setup conditional check
cpu : Add missing ' static ' attribute to qemu_global_mutex
vl : Add failure check for SetEvent
block : Replace u_int8_t , u_int16_t , u_int32_t , u_int64_t by standard int types
aio : test node -> deleted before calling io_flush
hw / smc91c111 : Add explicit ' return ' rather than relying on fallthrough
pseries : Don ' t allow TCE ( iommu ) tables to be registered with duplicate LIOBNs
gtk : set free_scale when setting zoom_fit
qcow2 : Fix refcount table size calculation
posix - aio - compat : Plug memory leak on paio_init () error path
pci : memory leak of PCIDevice :: rom_file
ahci : fix buffer overrun on invalid state load
petalogix_ml605_mmu : Attach ethernet to machine
hda - audio : fix non - mixer codecs
s390 - ccw . img : Fix sporadic reboot hangs : Initialize next_idx
vl : free err
rtl8139 : skip offload on short Ethernet / IP header ( CVE - 2015 - 5165 )
ide : Add forgotten VMSTATE_END_OF_LIST in subsection
lm4549 : Fix buffer overflow
virtio : Plug memleak by freeing vdev
mips : missing syscall returns wrong errno
target - tricore : Fix LOOP using wrong register for compare
sheepdog : fix NULL dereference in sd_create
cirrus_vga : remove unneeded reset
sheepdog : fix snapshot tag initialization
tcg / optimize : fix setcond2 optimization
block : Add SG_IO device check in refresh_total_sectors ()
ccid - card - emul : do not crash if backend is not provided
qcow2 : Free allocated L2 cluster on error
s390 / kvm : Add check for priviledged SCLP handler
vnc : Don ' t assert if opening unix socket fails
tcg - i386 : Allocate call - saved registers first .
block : keep bitmap if incremental backup job is cancelled
qemu - char : report udp backend errors
vt82c686 : avoid out - of - bounds read
block : Catch ! bs -> drv in bdrv_check ()
vhost - user : verify that number of queues is non - zero
qcow2 : Reject too large header extensions
target - s390x : Add missing tcg_temp_free_i64 () in disas_s390_insn (), opc == 0x8e
virtio - 9p : Check the return value of llistxattr .
qcow2 : Free preallocated zero clusters
exec : always use MADV_DONTFORK
xhci : add xhci_cap_write
fw_cfg : add required header files
block / iscsi : fix uninitialized variable
xen_disk : mark ioreq as mapped before unmapping in error case
target - mips : remove POOL48A from the microMIPS decoding
sheepdog : fix compile error on systems without TCP_CORK
slirp : Fix packet expiration
handle SD CMD5 without error messages
ehci : schedule async bh on async packet completion
rtl8139 : skip offload on short TCP header ( CVE - 2015 - 5165 )
hw / intc / arm_gic : Fix NVIC assertion failure
target - mips : log instructions start in TCG code
x86 : fix miss merge
sysbus : make SysBusDeviceClass :: init optional
spapr : Provide an error message when migration fails due to htab_shift mismatch
bb # 11514 - change cli_malloc with cli_calloc
bb # 11135 - sigtool now verifies cvd prior to unpacking
bb # 11129 - applied unencoded '@' patch
handle CL_ETIMEOUT in cl_strerror
don ' t cache possible unwanted
hwp3 . x : enable password cancelling of parsing
CID 12072 - added mssing return
add missing ' break '
Deal with similar wrap around in literal_len in the src buffer
Related : rhbz # 1602527 add comment to silence leaked_storage for ' glyphs '
libsmartcols : fix stupid typo
partx : work properly with 512 sectors ( dos PT )
utmpdump : make analyzers happy
fdisk : non - linux support ( MAXPATHLEN )
lscpu : avoid double free
fdisk : add support for the Xtensa architecture
setpwnam : remove non - ANSI definitions [ smatch scan ]
lib : [ procutils . c ] improve robustness
libblkid : don ' t gather PART_ENTRY_ * values for binary interface
rev : Avoid calling free in a signal handler
sulogin : don ' t run if not found passww entry [ coverity scan ]
login - utils : remove unwanted newlines from wall
Unload the GeoIP db when disabled .
Fixed typo reported at github # 1552
Settings : add missing save call for the ' shortcut / gkey ' setting .
accept NULL as a valid prefix in rpmdbOpen ()
Fix a small memleak in rpmsign tool
fix parsing of empty doublequoted strings in spec file ( rhbz # 456103 )
fixed a memory leak
Log an error on attempt to sign V3 packages ( RhBug : 517818 & others )
don ' t assume write ( fd , buf , 0 ) always returns w / o error
rpmdbRemove () should free header it read
bc_eval : legacy scripts may not contain some string list options
sync : don ' t count EXPUNGED record annotations in CRC
libconfig : more universal fix for logging fopen () error
jmap : free memory of ical component clone
http_caldav . c : dont leak userid in caldav_delete_cal ()
jmap : fix typo in calendarEventsSet response name
mboxlist : fix uninitialised memory use where pattern is " Other Users "
jmap : getMailboxes : fix sortOrder memory leak
mailbox : add mkdir back to copyfile , and return IOERROR
mboxlist : don ' t crash without patterns
strip batchsize on full sync
snippets needs a mailbox at the moment too
dav : don ' t double - close davdb on mailbox delete
mboxname : free recipient in cleanup
lmtpd : use xmalloc
mailbox : cleanup correctly when DELETED records retained
http_caldav_sched : handle blank attendees
cache : if fd has gone away , make sure mmap is clean too
search : always index the target of a rename
http_dav : allow read - free - busy to be set via DAV
prot : disable boundaries for now
http_caldav . c : don ' t allow an attendee to reply to a non - existent invitation
carddav : don ' t crash on x - fm - otheraccount - member with no userid
ctl_zoneinfo . c : skip any files that we can ' t parse as iCalendar
mboxlist : don ' t fail on subscribed but non - existent folder
http_dav . c : reset txn -> buf in MKCOL after we ' re done creating XML response
dav : zero out index_state
resolved a problem with delayed loading of keys
Modules : fix client blocking calls access to invalid struct field .
Cast sentlen to int before comparison wit bufpos .
cli : Only print elapsed time on OUTPUT_STANDARD
Streams : return an error message if using xreadgroup with '$' ID .
adding check error code
Scripting : Reset Lua fake client reply_bytes after command execution .
Fixed possible buffer overflow bug if RDB file is corrupted .
rtmpproto : Validate the embedded flv packet size before copying
shorten : set invalid channels count to 0
avplay : clear pkt_temp when pkt is freed .
avcodec / fic : clear slice_data
fixing segfault with http :// sam . zoy . org / zzuf / lol - ffplay . avi
avfiltergraph : set deprecated filter_count .
mp3adu : Set the channel layout properly
udp : Fix local_port management
indeo4 : fix out - of - bounds function call .
lavf : fix the comparison in an overflow check
workaround apps which dont provided correctly padded streams
avplay : Check frame allocation inside video_thread ()
put_bits : fix invalid shift by 32 in flush_put_bits ()
movenc : Free the buffer returned by url_close_dyn_buffer , regardless of the size
mpegtsenc : use correct PES stream_id for AAC
mov : Add support for zero - sized stsc runs .
dca : Validate the channel map
avconv : fix off by one check in complex_filter
sipr : Use memmove () to copy overlapped buffers .
rtmppkt : Make pkt -> data reallocable
xa : fix end - of - file handling
flvdec : Check for overflow before allocating arrays
qsvenc_hevc : use the correct HW plugin UID
flacdec : set default sample_fmt before parsing extradata
tls : fix compilation when both gnutls and openssl are enabled
fixing interlaced dct with 4mv
fix size of header data
remove unnecessary shift
rtpdec_vp9 : Make sure to free the temp buffer on close
dfa : check for invalid access in decode_wdlt ().
vsrc_movie : fix leak in request_frame ()
vc1dec : Override invalid macroblock quantizer
mpegts : Make sure we don ' t return uninitialized packets
png : Support RGBA64 pixel format
vf_gradfun : fix uninitialized variable use
libxavs : add an AVClass at the beginning of XavsContext .
dash : Use correct ISO C scanf conversion specifier
anm : prevent infinite loop
libopenh264enc : Return a more sensible error code in some init failure paths
adts : Fix PCE copying .
Correct the threshold of get_ur_golomb_jpegls () at which the optimized
xxan : don ' t read before start of buffer in av_memcpy_backptr ().
a64 : check that extradata exists before reading from it
proresdec : Properly make sure an index doesn ' t run past the limit
mpegmux : add stuffing to avoid incomplete PCM frames
rtpenc_chain : Use the original AVFormatContext for getting payload type
avio : Apply avoptions on the URLContext itself as well
avprobe : Zero the allocated avio buffer memory
h264 - mt : fix deadlock in packets with multiple slices ( e . g . MP4 ).
tinfoil patch : accept no widths that are no multiples of 4
clear the whole vert_pred buffer for 24 bit decoding .
mov : fix stream extradata_size allocation
handle http error codes
avcodec : set avpkt -> size to 0 if encode2 () did not output a packet
sol : return error if av_get_packet () fails .
reset interlaced_dct flag
Fix a read past end of buffer crash in the mp3 probe
mpegts : Silence " can ' t seek " warning on unseekable
vaapi_h264 : Add missing return value check
used defined name for testing error resilience level
vc1 : Do not return an error when skipping b frames
oma : check avio_read () return value
Removes misleading const qualifier , gets rid of two compiler warnings
cbs_h2645 : Return error if writing fails
simplify format string for writing pix_fmt string .
avidec : fix a memleak in the dv init code .
audemux : Add a sanity check for the number of channels
hlsenc : use the basename to generate the list entries
disconnect client if trailer is sent
cbs_h2645 : Fix memory leak on when reading SEI fails
Quench reject_delay to be no less than zero
Releases don ' t get replied to
Fixed md -> grouplist memory leak .
imcb_file_send_start : handle ft attempts from non - existing users
purple : Fix crash on ft requests from unknown contacts
Catch an infinite loop in smpp_handle_dlist (). Fixes bug 246 .
Call the " without FCS " version of the Ethernet dissector .
GTK : Bugfix not allowing extcap capture filters to be verified .
RTPS : fix Result of operation is garbage or undefined
wlan_stat : fix a memory leak
gcc - 4 . 1 . 1 thinks ' retval ' might not get set inside the TRY (). Initialize it .
dirty hack to make things compile without libgcrypt
Qt : fix use of uninitialized variable
frame_data : Reset subnum if visited is resetted
avoid a crash when tree is null
tvbuff : fix compilation with gcc - 4 .
decode as : Fix small memory leak
Initialize all tvb values in FieldInfo_get_range .
wsutil : return false if NULL is asked as profile name ( CID : 1405202 ).
DCT2000 : prevent a stack buffer overflow
packet - sip . c : 1469 : warning : unused variable ' contact_param_end_offset '
DICOM : Revert 3752a643ac5c85622822c88c69d83ce23828acb8
epan_init (): make ' status ' volatile ( to avoid might - be - clobbered warnings ).
gtk : fix memory leak in profile dialog .
dot11decrypt : free memory on exit ( found by clang ).
[ SUA ] Change the dissector table sua . prop . tags to use DISSECTOR_TABLE_ALLOW_DUPLICATE
Qt : Add a bottom border to the packet map .
plugin_if : fix no previous prototype for ‘ ext_toolbar_find_item ’ [- Wmissing - prototypes ]
bgp : add return check ( CID 1354417 ).
Try to make Mac OS X Buildbot Happy
tshark : fix double memory free when analyzing several RTP streams
Qt : Fix leaked interfaces array in Wireless frame
Stuff the " Don ' t show this message again " checkbox in the dialog ' s
extcap : add more checks for extcap disabled .
ws_pipe : Zero an array .
qnet6 : don ' t calculate crc if length is negative
Put volatile back - without it , the compiler warns you .
Break out of an infinite loop . Fixes bug 95 .
extcap : fix a compile error with Qt4
get_persconffile_path () return malloc ' d memory ,
RTP : Parse problem in dissect_rtp_hext_rfc5215_onebyte
Qt : fix buffer alarm threshold parameter retrieval in multicast statistics dialog
RADIUS : Initialize vendor attribute type to zero
Qt : add initializer ( CID 1230901 ).
Add another check to radiotap - iter initialization to catch another out - of - bounds
Fix a memory leak which occured upon registering an already existing
Fixed an iconv_strrpos () bug that the function doesn ' t return accurate
fix potential leak
Added check for calloc () failure
don ' t read empty files
fix typo and bug # 41962
cannot import nodes without document
keep previous test to avoid any behavior change
Fixed incompatible pointer type
forgot to decrement result_allocated :(
Fixed NULL pointer dereferencing
Set the correct function state during execution . This is mainly to have
fix yet another possible segfault
Initialize all struct members : Necessary for reflection
fix : prevent possible crash
fix yet another leak
fix memory leak
fix memory leak
replace llabs with imaxabs
fileinfo : fix misleading indentation in libmagic
Freeing the memory would be a good idea ...
IE we love you all ...
cleanup unused variable
Make it possible to disable post_max_size
oops . Copy - paste - magic . :)
fix for bug # 32282 ( segfault in mysqli_fetch_array on 64 - bit )
Added missing charset .
Fixed assignment of reference to itself
Prevent opening of empty filename ""
Fixed bug codes in SplFileInfo :: getExtension
Added an E_NOTICE when precision value is truncated
Fixed a possible memory leak on open_basedir validation
fix segfault when running run - tests . php , and general potential for corruption
efree match_sets on an error here - found by Coverity
Fixed memory leak
prevent flags overflows
efree match_sets on an error here - found by Coverity
make ftp compile on unix again
support sucking data from streams in file_put_contents ()
dir : Reserve the whole @@ prefix
lib : Make flatpak_installation_drop_caches return TRUE on success
[ common / flatpak - run ] add / dev / mali0 to -- device = dri
helper : Allow exec on the tmpfs .
flatpak remote - ls - u : only consider apps from the current remote
run : Don ' t allow chroot ()
flatpak_number_prompt : Add missing va_end
run : Don ' t allow unmounting filesystems
create - usb : Fix a leak of the list of extensions
xdp - fuse : Add parentheses to clarify precedence in a conditional
synIDattr (): true color awareness
vim - patch : 8 . 0 . 0584 : memory leak when executing quickfix tests
viml / executor : Fix check - single - includes
Fixed a memory leak with alternative names
winpr - handle : fixed mem leak when closing named pipes
core : Added missing \ 0 termination to crypto_print_name
core : free polyline and polygon_sc points
Fixing issue 245
Be on the safe side when freeing stuff
Parser : be more conservative when estimating the line size
fuzz - unit - file : allow a space between variable name and "=" (# 8670 )
systemctl : add missing OOM check
journalctl : properly detect empty journal files
journalctl : show fields requested with -- field in full
sysusers : fix memleak (# 4430 )
systemctl : fix detection of active units
ude_rules . c : fix possible NULL pointer dereference in get_key ()
kdbus : make sure msg structure is properly initialized
[ PATCH ] udevd : fix valgrind warning
tmpfiles : restore previous behavior for F / f
resolved : merge two bools into a bitfield
modules - load : filter out double modules
systemctl : also highlight a load state of " not - found " as red
systemctl : return diffrent error code if service exist or not (# 3385 )
locale : add printf attribute to log_xkb ()
systemctl : fix memory leak in systemctl_parse_argv
systemctl : fix check for template units
vconsole : copy font to 63 consoles instead of 15
journal - upload : allow the tool to start
sd - event : fix theoretical leak on error in event_make_signal_data
sd - bus : let ' s use mfree () where we can
systemctl : do not print header if no units will be listed
vconsole : add copyright line
machinectl : don ' t show ". host " pseudo - machine in list by default
systemctl : allow listing cgroups of containers
resolved : use single message for both dbus and signal calls (# 3515 )
resolved : don ' t override zone item state after starting the probe
util : ignore_file should not allow files ending with '~'
libudev : always add UDEV_LOG
journalctl : free arg_file on exit
shared : handle unnamed sockets in socket_address_equal ()
bus : properly unset default bus pointer when destroying last reference
locale : make sure that l is freed
udev : link - config - fix naming policy
journal : make sure to initially populate the space info cache (# 4807 )
base - filesystem : avoid all searching if the link already exists
build - sys : make gcc shut up
rtnl : fix for 32bits
udev : properly detect reference to unexisting part of PROGRAM ' s result
IMPORT : 2048 -> 4096 bytes buffer
udev : kmod - fix typo
logind : never select closing sessions for a VT
path - util : when parsing a timestamp we don ' t know the timezone
ask - password : use kill ( PID , 0 ) before querying a password
journalctl : don ' t choke on entries with no MESSAGE = field
json : avoid cleanup of unitialized variable
virt : make sure we don ' t ignore some errors
sd - rtnl : rtnl_call - don ' t dispatch wqueue after timeout has passed
logind : prefer new sessions over older ones on VT switches
silence " comparison between signed and unsigned "
selinux : reopen log when loading the SELinux policy fails
dhcp - server : fix integer underflow
core / transaction : fix cycle break attempts outside transaction
Drop the DH group check to a weaker version of the checks
roken_detach_prep () close pipe
k_gethostname -> gethostname
allocate cleam memory for cipher to play with
check that ctx is not a null ptr before deref cid # 140
Prototypes for keyfile and unseal_key .
add -- no - embedded - cert and -- embed - leak - only
make compile again
improve msft compiler case
select heimdal_kuser as text domain
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
rdpei : Fix possible NULL ptr access
xfreerdp - client : xi tweak2
Fixed malloc with 0 size .
core : free polyline and polygon_sc points
Fixed null pointer dereference .
core : reset crypto key counters when the keys are initialized
libfreerdp - core : fix server crash in case rdp key file is not provided .
libfreerdp - utils / args : fix wrong argument index for plugin name .
Fixed # 6005 : Bounds checks in update_read_bitmap_data
libfreerdp - core : server fix sending zero length update pdu .
Fixed oob read in ntlm_read_NegotiateMessage
Fixed GHSL - 2020 - 101 missing NULL check
Fixed missing stream length check in drive_file_query_directory
rdpsnd : follow rdpdr and free the stream from svc_plugin
libwinpr : check return value of realloc .
winpr / file : add missing NULL check
Fixed missing variable type
Deactivated glyph cache .
Fixed Out - of - bound read in glyph_cache_put
libfreerdp / core : transport_write unchecked parameters
Fixed leak of client random .
flooddetach : add nick changes - close # 941
fix php 72494 , invalid color index not handled , can lead to crash
remove unit test spam from mongos
fix authorization_manager_test build
assert that you can ' t shard config db
Init member ( thanks , valgrind ).
Document a parameter .
std :: vector :: data () is not standartized and on windows it cause build failure .
avformat / asfdec_f : Correct skip to key code
mpeg4 : report frame decoding completion at ff_MPV_frame_end ().
dv : zero dsp before init , this fixes use of uninitialized dct_bits
avformat / movenc : Check input sample count
wavpack : Fix an integer overflow
avcodec / utils : Better check for channels in av_get_audio_frame_duration ()
indeo5 : check against scaleable frames in non scaleable streams .
dxva : DXVA2_ModeHEVC_VLD_Main10 does not support Main
fix segfault with lol - ffplay2 . mpg ( dunno if this is exploitable , probably not easily )
h264_cavlc : check the size of the intra PCM data .
avcodec / options : use av_opt_copy () in avcodec_copy_context () to copy priv_data
vf_hqdn3d : fix uninitialized variable use
cdgraphics : initialize buffer
electronicarts : prevent overflow during block alignment calculation
add support for PCM codec
avformat / utils : fix calculating the absolute difference of timestamps
dxva : DXVA2_ModeHEVC_VLD_Main10 does not support Main
avcodec / vorbis : 1 << 31 > int32_t :: max (), so use 1u << 31 instead .
avcodec / dcadec : add an assert to silence a uninitialized variable warning
avcodec / pthread_frame : Fix memleak of AVCodecContext on error
avcodec / atrac3 : Check init_get_bits8 () for failure
avformat / rmenc : Check for av_malloc () failure
check qblur > 1 ( prevent assert failure / segfault )
avformat / avidec : initialize header array
mvdec : check var_read_string () return value
fix crash on non - AltiVec powered machines : MPV_common_init_altivec doesn ' t check mm_flags
lavu / opt : dont crash on freeing NULL AVOptionRanges
brstm : fix use of uninitialized variables
smc : Fix overread .
hevc_cabac : decrease CABAC_MAX_BIN
arm : fix use of uninitialised value in ff_fft_fixed_init_arm ()
tests / audiomatch : Fix compile warning
pthread_frame : Unreference hw_frames_ctx on per - thread codec contexts
avformat / nutdec : fix packet end clearing
Make the RTSP muxer use rtsp_hd_out , for consistency
oggparsevorbis : Remove code messing with cur_dts .
tscc2 : fix out of array access
lavu / opt : validate range before dereference
ffprobe : fix use of uninitialized variable
avcodec / lcldec : initialize encoded correctly
lavfi / hqdn3d : avoid use of uninitialized variable .
wmavoice : Check for out of bound writes
avcodec / hevc : Silence " warning : ref0 / 1 may be used uninitialized in this function "
pngdec : check for bprint finalization sucess on icc data parsing
mpc8 : fix channel checks
avcodec / hevc : clear filter_slice_edges () on allocation
avcodec / binkaudio : clear padding area of packet_buffer
tests / checkasm / checkasm : Give macro a body to avoid potential unexpected syntax issues
avformat / vfq : fix deadlock in vqf_read_header ()
libavformat / mxfdec : fix MXFPackage -> comment_refs memory leak
Loop up to MAX_THREADS instead of h -> s . avctx -> thread_count to free the thread
svq3 : propagate codec memory allocation failure in context init
avcodec / movtextdec : run mov_text_cleanup () before overwriting pointers
ff_h264_direct_ref_list_init : fix B slice check .
avcodec / lagarith : Fix runtime error : left shift of negative value - 1
url_alloc_for_protocol : fix use of uninitialized variable
avformat / rmdec : zero string destination before use
avcodec : fix uninitialized variable read
snowenc : dont crash with gray but exit with an error msg .
avformat / mpegenc : Check for av_mallocz () failure
avformat / hls : fix memory leak with non - http segments
ffprobe : fix crash in case - of is specified with an empty string
Check the allocated pointer instead of the given pointer as intended
truemotion1 : fix leaking frame on init failure
avformat / mpegts : check avio_tell () return code before using it
avcodec / aacdec_fixed : fix : left shift of negative value - 1
v410dec : Check for sufficient input data .
vf_lut : make config_props work properly when called multiple times .
avformat / matroskadec : check generic audio deinterleaver sub_packet_size against frame_size
matroskadec : fix integer overflow
avcodec / ivi : Free custom blk_vlc
ffprobe : check av_frame_alloc () failure .
jpeglsenc : Mark codec as init - thread - safe and init - cleanup
indeo4 : Fix global array overread .
avsdemux : check for corrupted data
gifdec : gif_copy_img_rect : Fix end pointer
avformat / utils : avoid overflow in update_stream_timings () with huge durations
avcodec / sanm : Reset sizes in destroy_buffers ()
avformat / asfdec_o : Check size_bmp more fully
avcodec / wavpack : Fix runtime error : left shift of negative value - 1
h264 : don ' t initialize missing pictures when using VDPAU .
avsdemux : check for out of bound writes
vformat / tee : fix uninitialized use of ret
avcodec / flacdec : Fix signed integer overflow in decode_subframe_fixed ()
gxfenc : fix null ptr dereference
pass strict_std_compliance to audio context too
h264 : unref leaking error recovery picture
avcodec / g726 : Fix runtime error : left shift of negative value - 2
fraps : check for overread
oggdec : Fix memory leak in setting up vorbis headers
webmdashenc : Fix potential memory leak
dca : Read params -> pancABIT0 [ param_index ] only if part0 is set
svq1enc : fix signed multiplication overflow
vorbis_parser : Move vp check to avoid a null pointer dereference
bessel : check that the function terminates as expected by av_assert2 ().
psymodel : dont apply lowpass filters with a cutoff close to the nyquist
snowenc : check minimum resolution
avformat / http : Use av_freep () avoid leaving stale pointers in memory
avcodec / jpeg2000 : Fixes integer overflow in ff_jpeg2000_ceildivpow2 ()
lavfi / frei0r : fix crash when parameters are not set .
mpegvideo : fix null pointer dereference on switching streams
avcodec / movtextdec : Call mov_text_cleanup () on close
asplit : don ' t leak the input buffer .
libavcodec / exr : fix channel size calculation for uint32 channel
avcodec / mdec : Fix runtime error : left shift of negative value - 127
avutil / frame : fix crash with av_frame_unref ( NULL )
matroskadec : only return corrupt packets that actually contain data
bktr : fix warning : missing braces around initializer
ffmpeg : fix handling or empty audio packets
ffserver : avoid leaking pathname at exit
adpcmenc : check for coded_frame allocation failure
avcodec / mpegaudiodec_template : Check for negative e
avformat / concatdec : Enable auto_convert by default
sgi : check maximum supported resolution
avidec / guess_ni_flag : fix harmless integer overflow
avcodec / ffv1dec_template : Fix signed integer overflow
qt - faststart : Avoid leaking memory if encountering a file with double ftyp atoms
h264 : allow debuging SEI types
avformat / utils : check for overflow before reallocating side data
mpegvideo : clear overread in clear_context
avcodec / imc : Fix bitstream buffer padding
riff : fix invalid av_freep () calls on EOF in ff_read_riff_info
samplefmt : avoid integer overflow in av_samples_get_buffer_size ()
swresample / resample : Limit filter length
pthread : don ' t increment index on zero - sized packets .
mpeg12 : fixed parsing in some mpeg2 streams
adxdec : Validate channel count to fix a division by zero .
hls : fix integer overflow
avcodec / shorten : Sanity check maxnlpc
avcodec / hevc_sei : fix use of uninitialized variables in decode_nal_sei_frame_packing_arrangement ()
motionpixels / mp_decode_frame_helper : assert that the first pixel doesnt reuse the last .
avformat / mov : Check extradata before access
avcodec / wavpack : initialize pointers to silence warning about them possibly being uninitialized
libavformat / http : Fix memory leak in get_cookies .
avcodec / tiff : Check for multiple geo key directories
wtvdec : fix integer overflow
doc / examples / decode_video : Fix format string vulnerability
avcodec / avpacket : Check metadata key in av_packet_unpack_dictionary ()
doc / examples / http_multiclient : Fix resource leak
lavf / tee : initialize ret in parse_bsfs ()
avcodec / interplayvideo : Check that enough data is available for opcode_0xA
avformat / flac_picture : clear padding area
electronicarts : check bytes per sample for validity
avcodec / diracdec : Fixes integer overflow
avcodec / wmv2enc : allocate padding for extradata and check malloc failure
flvdec : check return value of create_stream ()
lavc / utils : fix extra ASS sanity check in convert_sub_to_old_ass_form ()
bmp : Fix warning X may be used uninitialized in this function
twinvq : validate that channels is not <= 0
eatqi : replace break by goto .
avcodec / rv34 : fix crash while seeking on very damaged file
hwcontext_d3d11va : fix crash on frames_init failure
mpeg4videodec : Fix " warning : dc_pred_dir may be used uninitialized in this function "
threads : Perform the generic progress cleanup more carefully .
avcodec / mpeg4videodec : Check for bitstream overread in decode_vol_header ()
pcx : round up in bits -> bytes conversion in a buffer size check
sapenc : Free AVStream -> info on cleanup
Shut up an uninitialized variable GCC warning in VP8 .
avcodec / dvbsubdec : Check for duplicate regions in dvbsub_parse_page_segment ()
wma : Lower the maximum number of channels to 2
mpegvideo_enc : reduce QMAT_SHIFT to avoid overflow in dnxhd
avformat / dump : Fix context / level for payload dump
lavc / avpacket : check for malloc failure
asrc_aevalsrc : Fix use of uninitialized pointer inside av_strtok ()
fix infinite loop ( suggested change by rjayne at convera dot com )
avformat / rtspdec : Use av_mallocz_array ()
ffmpeg : check avpicture_get_size () retuen value
avcodec / mpegaudiodec_template : use av_freep (), do not leave stale pointers in memory
add check for invalid channel mode when coupling is used
avformat / vqf : check a few more bits in probe
avformat_alloc_context : initialize raw_packet_buffer_remaining_size .
wma : initialize prev_block_len_bits , next_block_len_bits , and block_len_bits .
asfdec : set nb_streams to 0 in the asf_read_close
avformat / http : return EOF from ff_http_do_new_request if previous response said Connection : close
drawtext : fix leak with timecodes
avcodec / g2meet : Fix order of align and pixel size multiplication .
avcodec / diracdec : Fix integer overflow in INTRA_DC_PRED ()
avformat / omadec : Use 64bit for ret to avoid overflow
avformat / movenc : Write version 2 of audio atom if channels is not known
tta : fix off be 1 error in the end detection .
AMV : Fix possibly exploitable crash .
asfdec : prevent possible memory leak in the asf_read_metadata_obj
avcodec / mpeg12dec : Check for overread in mpeg_decode_slice ()
flvdec : Check for overflow before allocating arrays
lavfi / scale : allocate interlaced scalers only if needed .
avcodec / jpeglsdec : assert that overflows end at exactly x = w
h264 : don ' t sync pic_id between threads .
matroskadec : Ensure time_scale is nonzero , fixes divide - by - zero if the file
wmavoice : initialize best_hist_ptr to NULL to prevent ( incorrect ) warning .
mpegaudiodec : Validate that the number of channels fits at the given offset
avformat / avidec : Limit formats in gab2 to srt and ass / ssa
avformat / format : Check for av_guess_format () failure
h264 : error out on unset current_picture_ptr for h -> current_slice > 0
avformat / movenc : Do not pass AVCodecParameters in avpriv_request_sample
jpeg2000dec : Check bpno in decode_cblk ()
fix vdpau vc1 interlace modes
lavd / alsa - audio - common : mark default device in device list
rtpdec_qdm2 : Don ' t try to parse data packet if no configuration is received
avcodec / get_bits : add av_assert2 to get_bits_long ()
avcodec / mpeg12dec : check block index in mpeg2_fast_decode_block_non_intra ()
lavf / pcm : check size , do not produce invalid packets
avcodec / hevc_refs : fix potential use of uninitialized min_idx
avcodec / truemotion2rt : Check input buffer size
amovie : avoid crash in amovie_request_frame () if no samplesref is available
pthread_frame : fix uninitialized variable read
Remove a NAL unit ' s trailing zero bytes even when dst_length is 1 .
Fix a crash caused by a null coded_picture pointer in ffserver .
wmaenc : require a large enough output buffer to prevent overwrites
avcodec / put_bits : Assert that size in set_put_bits_buffer_size () does not cause integer overflows
mpegpsenc : show first SCR / DTS at debug level
ape : Fix null ptr dereference with files missing a seekatable .
libstagefright : avoid memory leak
avcodec / get_bits : Fix get_sbits_long ( 0 )
avformat / mpc8 : fix broken pointer math
avcodec / pthread_slice : Use av_freep () to avoid leaving stale pointers in memory
mpevideo_enc : disallow multiple slices for h261 and flv
avcodec / ffv1dec : Check for 0 quant tables
avcodec / h264_refs : Do not set reference to things which dont exist
librtmp : append the correct field to the string
flashsv2_prime : check block before using it .
avutil / lzo : Fix integer overflow
utils : fix integer overflow with DURATION_MAX_READ_SIZE
avcodec / mlpdec : Fix : runtime error : left shift of negative value - 8
avcodec / adpcmenc : fix integer overflow / undefined behavior in STORE_NODE ()
avformat_open_input (): Add braces to shut up gcc warning .
lzw : check for overread
swscale : fix another integer overflow at large dimensions / rescales .
lavfi / select : make sure avctx is set before closing it .
avcodec / lagarith : disable lag_decode_zero_run_line () and ask for a sample
avcodec / hevc : use av_mallocz () for allocating tab_ipm
ffplay : Fix usage of private lavfi API
avformat / ipmovie : check OPCODE_INIT_VIDEO_BUFFERS size more completely
Check for out of bound writes in the wmavoice decoder .
h264 : Fix memory leak on ff_h264_decode_init () failure
[ PATCH ] Fix crash when initializing multi - threaded decoding for corrupted file .
avformat / icodec : reduce score returned on probing
Fix an overflow in the predictor .
rv34 : fix signed multiplication overflow
golomb : fix end of bitstream check
avcodec / utils : Fix signed integer overflow in rc_initial_buffer_occupancy initialization
unset Track sets , relying in AVStream -> priv_data and will be freed in av_close_input_file
rtpproto : Check the size before reading buf [ 1 ]
avcodec / mjpegdec : zero gb to silence warning about it being possibly uninitialized
Fix a warning on an uninitialized variable .
lavc / alac : Avoid allocating huge memory blocks for malicious alac input .
aacdec : Fix null pointer crash
proresenc : Write the full value in one put_bits call
libwebpenc_animencoder : zero initialize the WebPAnimEncoderOptions struct
aiff : check block_align in aiff_read_packet
lavfi / unsharp : fix opencl crash on 64bit linux
w32threads : with only 1 thread there cant be a active threading mode .
libswscale / tests / swscale : Fix uninitialized variables
mov : immediately return from mov_fix_index without old index entries
msrle : Use FFABS to determine the frame size in msrle_decode_pal4
lavfi : avfilter_merge_formats : handle case where inputs are same
ensure av_rescale_q () can be calculated ( won ' t divide by zero )
avcodec / ituh263dec : Use 0xffff as error code in h263p_decode_umotion ()
avcodec / dca_lbr : Fix off by 1 error in freq check
h264 : Fix parameters to ff_er_add_slice () call
smoothstreamingenc : explict cast to avoid overflow
avcodec / mlpdec : Fail if the input is too small
lavf / mpeg : suppress warning : lpcm_header_len may be used uninitialized in this function
avformat / utils : avformat_find_stream_info fix a crash in case of oom
alsdec : only adapt order for positive max_order
af_channelmap : fix uninitialized variable use introduced in ba8efac977f4276f05274947b2b67d144cbc965a
avcodec / asvdec : Check buf_size
hwcontext_d3d11va : fix crash on frames_init failure
avformat / mxg : Use memmove ()
vaapi_h264 : Add missing return value check
lavc / hevcdec : fix invalid use of ff_get_format ()
vf_overlay : silence warning : X may be used uninitialized in this function
avutil / frame : fix av_frame_copy for unknown layouts
ffplay : fix mem leak when opening input or parsing options fail .
avcodec / mpeg4videodec : Fix GMC with videos of dimension 1
bmv : check for len being valid in bmv_decode_frame ().
indeo5 : fix division by 0 in ff_ivi_init_tiles ()
pca : use sizeof ( variable ) instead of sizeos ( TYPE )
lavd / v4l2 : do not fail when VIDIOC_ENUMSTD returns EINVAL without a valid match
fix double free , priv_data is freed in av_open_input_stream
avprobe : Fix memory leak
rsd : limit number of channels
ffmpeg : fix sws_dict leak on error exit
avcodec / h264_slice : Clear ref_counts on redundant slices
lavf / tee : fix side data double free .
ffv1 : set internal frame defaults
dca_parser : don ' t overwrite the sample rate , it may not be correct
bitrate sanity check ( fixes assertion failure )
revert r8469 ( increase bits for non default highprecision mode )
lavc : do not leak the internal frame if opening the codec fails
wavpack : check K , fix assertion failure
avcodec / utils : Fix undefined behavior in avpriv_toupper4
libtwolame : prevent a NULL pointer dereference
pmpdec : fix integer overflow
lavf : fix null pointer dereference in rdt
mpegts : fix stack array overread in read_sl_header ()
Fixed segfault on memory allocation failure in ape demuxer .
libavutil / opencl : fixed uninitialized var warning
h264 - mt : fix deadlock in packets with multiple slices ( e . g . MP4 ).
lavf / concatdec : fix crash in case a file in script cannot be opened
avcodec / vp56 : Check avctx -> error_concealment before enabling EC
fate / api - codec - param : fix codec context leak
avcodec / libxvid : fix potential integer overflow
h264 : Check that the codec isn ' t null before accessing it
truemotion1 : use av_freep ()
golomb : Fix overread in get_ur_golomb_jpegls ()
avformat / sdp : fix memleak on error
avcodec / xpmdec : there are XPM files with dos line endings
avformat / pva : Check for EOF before retrying in read_part_of_packet ()
j2kdec : Fix integer overflow leading to a segfault
adpcm : Fix crash
kill uninitialised variable warning in amr_read_packet ()
avformat / vobsub : fix NULL dereference
ffprobe : fix NULL pointer dereference in writer_close ()
cdxl demux : do not create packets with uninitialized data at EOF .
smoothstreamingenc : Fix a memory leak on errors
avutil / libm : fix isnan compatibility hack
rtpenc_mpegts : Set chain -> rtp_ctx only after avformat_write_header succeeded
Fix a crash in the H . 263 RTP packetizer
avcodec / mips / aaccoder_mips : Disable ff_aac_coder_init_mips () to prevent build failure
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
avformat / gxfenc : Clear nb_fields on array deallocation
avcodec / h264 : clear cur_pic structure instead of duplicating it in ff_h264_update_thread_context ()
libx265 : Pass through user - set frame type
vc2enc_dwt : use 32 bit coefficients by default
avcodec / x86 / cavsdsp : Put MMX code under mmx check
avformat / mov : Fix DoS in read_tfra ()
h264 : initialize H264Context . avctx in init_thread_copy
avcodec / dirac_dwt : initialize support value for fidelity wavelet
vp3 : avoid buffer overread in coeff decode
mpc8 : check seektable size before attempting to use it .
lavfi / aevalsrc : fix sample rate negotiation , after the last merge
vp3 : Check fps validity more completely
indeo5 : fix null pointer crash with ref_mb
ffserver_config : Check for failure to allocate FFServerIPAddressACL
checkasm : vp8 . mc : initialize the full src buffer after ec32574209f
avcodec / mpegvideo : fix null pointer dereference
vp3 - mt : fix deadlock when first frame is not a keyframe .
Remove code that duplicates existing code a few lines down .
avformat / oggparsespeex : Check for extradata allocation failure
init picture structure to avoid uninitialized fields
avcodec / flacdec : avoid undefined shift
avcodec / pixlet : Fixes : undefined shift in av_mod_uintp2 ()
ffmpeg : use isatty () before messing with the terminal state
msmpeg4 : ignore negative DC overflow
lavf : Dont compare absolute to relative timestamps in duration gcd
ffmpeg_vaapi : Unreference global device before setting it
avutil / imgutils : correctly check for negative SAR components
svq1dec : call avcodec_set_dimensions () after dimensions changed .
avformat / mpc8 : clear buffer padding area
h264 : prevent overreads in intra PCM decoding .
avcodec / tiff : reset geotag_count in free_geotags ()
avformat / smacker : Check if bufs has been allocated before use
png : use av_mallocz_array () for the zlib zalloc function
lavf / mov : don ' t read outside frag_index bounds
Avoid a deadlock when decoding wma .
avcodec / mpegaudiodec_template : Make l3_unscale () work with e = 0
avformat / pcmdec : fix memory leak
random_seed : allow to block on / dev / random
ffmpeg_opt : Use av_realloc_array ()
proresdec : Properly make sure an index doesn ' t run past the limit
avformat / mov : Fix potential integer overflow in mov_read_keys
Avoid a null pointer dereference on oom when decoding smacker .
avcodec / webp : Fix signedness in prefix_code check
av_dict_set : fix potential memory leak with AV_DICT_DONT_OVERWRITE
avienc : fix overflow of audio sample count
avcodec / dcaenc : clear bitstream end
examples / muxing : check on frame
avcodec / movtextdec : Make tx3g_ptr unsigned
avcodec / mlpdsp : Fix undefined shift ff_mlp_pack_output ()
avcodec / hevc_cabac : decrease CABAC_MAX_BIN
ffplay : avoid frame data leak on early frame drop
avcodec / mpeg4videodec : Clear interlaced_dct for studio profile
avcodec / kgv1dec : Check that there is enough input for maximum RLE compression
Set start_time to 0 for raw audio files .
sapenc : Free AVStream -> info on cleanup
avcodec / rl2 : clear freed pointers
Fix a read past end of buffer crash in the mp3 probe
avcodec / scpr : Check for min > max in decompress_p ()
h264 : copy h264qpel dsp context to slice thread copies
avcodec / smc : Check remaining input
avplay : Don ' t try to scale timestamps if the tb isn ' t set
img2dec : dont set start_time / duration to invalid values
flv : Warn only once
swscale : align vertical filtersize by 2 on x86 .
decode : avoid leaks on failure in ff_get_buffer ()
avformat / mxfdec : fix typo in error check of mxf_edit_unit_absolute_offset ()
libavcodec / libtwolame : fix null pointer dereference
vda : better frame allocation
pthread : Change a signal to a broadcast as multiple threads
avutil / mem_internal : add missing header includes
avcodec / proresdec2 : SKIP_BITS () does not work with len = 32
http : Check for negative chunk sizes
matroskadec : Fix buffer overread in matroska_ebmlnum_uint
avcodec / wavpack : Fix integer overflow in wv_unpack_stereo ()
af_channelmap : free old extended_data on reallocation
avcodec / flac_parser : Check for av_malloc () failure
mxfenc : don ' t try to write footer without header
lavfi / abuffer : init the data planes with NULL pointers .
avcodec / dcaadpcm : check for av_malloc () failure
bink : check vst -> index_entries before using it
rtpdec_asf : Don ' t free the payload context in the . close function
vc1 : reset interlaced variables , prevent another bunch of crashes .
movenc : Free the buffer returned by url_close_dyn_buffer , regardless of the size
vf_unsharp : Fix false ‘ src2 ’ may be used uninitialized in this function warning .
aacsbr : always initialize max_qmf_subbands
avcodec / takdec : Fix runtime error : left shift of negative value - 63
mjpegdec : tighten unescaped_buf_size size check , prevent null ptr deref
v4l2 : fix uninitialized variable
lavfi / buffersink : check av_frame_ref () failure .
lzw : fix potential integer overflow .
avformat / mxfdec : Check size to avoid integer overflow in mxf_read_utf16_string ()
libopenjpegenc : add NULL check for img before accessing it
avformat / mxfdec : Fix Sign error in mxf_read_primer_pack ()
matroskadec : reset size when freeing data .
h264 : free the tables and uninitialize the context on flush
avcodec / mjpegdec : Fix integer overflow in DC dequantization
avformat / mxfdec : fix null pointer dereference
jv demuxer : prevent video packet size overflow
mpeg12 : check for available bits to avoid an infinite loop
avcodec / vorbis_parser : Move vp check
avcodec / amrwbdec : Fix division by 0 in voice_factor ()
dcadec : check xch_base_channel against channel_order_tab .
h264 : check buffer size before accessing it
cmdutils : fix null pointer dereference
aacdec : drop channel reseting code .
avformat / oggparsedaala : Do not leave an invalid value in gpshift
avcodec / wnv1 : clear padding area of rbuf
wma : fix off - by - one in array bounds check .
oggenc : fix " oggstream may be used uninitialized in this function " warning
hevc : fix wpp threading deadlock .
avcodec / mlz : clear dict on allocation to ensure there are no uninitialized values
diracdec : check if reference could not be allocated
avcodec / escape124 : Check depth against num_superblocks
lavc / avfft : init context to 0 .
avcodec / lagarith : fix integer overflow
lavc / videotoolboxenc : Fix crash when closing codec after error
avformat / movenchint : use realloc_array () to protect against integer overflow
j2kdec : debug markers support .
avformat / aacdec : Fix leak in adts_aac_read_packet ()
h264 : Fix a possible overread in decode_nal_units ()
av_tempfile : fix leak in error case
avcodec / eatgv : use av_mallocz () for frame_buffer
lavc / qdrw : Do not fail decoding valid Quickdraw images .
avcodec / dfa : Fix off by 1 error
avcodec / ituh263dec : Fix runtime error : left shift of negative value - 22
typo me thinks ...
ffprobe : fix uninitialized variable warning
avcodec / smc : fix off by 1 error
avcodec / hevc_cabac : Fix integer overflow in ff_hevc_cu_qp_delta_abs ()
wtv : Mark attachment with a negative stream id
Fix out of bound reads in the QDM2 decoder .
ffm_seek : fix division by zero
avformat / cache : Use int64_t to avoid int overflow in cache_read
h264 : Fix assignments in if ()
a64multienc : change mc_frame_counter to unsigned
indeo4 : prevent printing uninitialized variable
avcodec / lagarith : Check scale_factor
xxan : fix invalid memory access in xan_decode_frame_type0 ()
swscale : enforce a minimum filtersize .
ffplay : zero initialize copy avpacket
avcodec / libxvid : Fix use of uninitialized AVPacket fields
tm2dec : fix overread
diracdec : avoid overflow of bytes * 8 in decode_lowdelay
adxenc : check return value of avcodec_alloc_frame ()
avcodec / xan : Check for bitstream end in xan_huffman_decode ()
avformat / sierravmd : check for memory allocation failures
decode : Initialize ret before using it
avformat / msf : Also check the codec tag in probing
mcdec : suppress " warning : a / vst may be used uninitialized in this function "
ra144enc : avoid integer overflows .
avformat / utils : free AVStream . codec properly in free_stream ()
wmv1 : check that the input buffer is large enough
avformat / ftp : Solve a crash bug when network occur a exception
lavf : Avoid using av_malloc ( 0 ) in av_dump_format
avcodec / wavpack : Fix 280 : 22 : runtime error : left shift of negative value - 1
avcodec / ac3_parser : Check init_get_bits8 () for failure
lavf / concatdec : do not transfer custom IO flag
xan : Prevent NULL dereference with missing palette
dvbsubdec : fix division by zero in compute_default_clut
ffprobe : replace av_destruct_packet () with av_free_packet ()
lavf / mov : Do not overread iTunes metadata .
avcodec / nvenc : set correct error code
prevent integer overflow in calculating duration
avcodec / ivi_common : use av_mallocz () to allocate mbs array
libopenjpegdec : always check image because decoding may still fail
pictordec : pass correct context to avpriv_request_sample
msmpeg4dec : check w / h , prevent assert failure later
avformat / segment : Pass flags to child context
avcodec / g2meet : check framebuf has been allocated before use
fix memory leak in oggenc
flvdec : fix creation of lots of phantom data streams
h261 : check mtype .
lavc / jpeglsenc : fix allocation in case of negative linesize , and add malloc check
mp3dec : Fix possibly exploitable crash
graphparser : add a NULL check on the argument passed to strstr
Fix out of bound writes in fix_bitshift () of the shorten decoder .
idcin : check for integer overflow when calling av_get_packet ()
rtspdec : Fix use of uninitialized byte
check entries against field_size , potential malloc overflow in read_stsz , fix # 1357
westwood_vqa : fix null pointer dereference
libavcodec / util : Fix timebase overflow check
libx264 : zero x264_picture before use .
avcodec / ra144dec : Fix runtime error : left shift of negative value - 17
avidec : Check that the header chunks fit in the available filesize .
avformat / gxf : Use 64bit for res to avoid overflow
avresample : Make sure the even check does not overflow
avutil / mem : Fix flipped condition
ffplay : do not drain existing filters when seeking
avcodec / hnm4video : check that packetsize is at least 8
bitstream : fix a crash in case of oom
avcodec / dxv : Check remaining bytes in dxv_decompress_raw ()
truemotion2 : error out if the huffman tree has no nodes .
avcodec / gdv : Fix undefined shift
lavfi / amerge : drop duplicated " Buffer queue overflow " message
vf_gradfun : fix uninitialized variable use
cook : silence some signed overflow warnings .
idcin : check chunk_size value before using it
avcodec / wavpack : Fix integer overflow
avcodec / jpeg2000dwt : Fix runtime error : left shift of negative value - 123
avcodec / crystalhd : allocate padding for extradata
fix memleak in packet corrupt dropping code
matroskadec : set cur_dts after seek
avcodec / mlz : Remove ' l ' postfixes from numbers
vc1dec : fix block_off
vqf : check samplerate , avoid division by 0 .
binkaudio : fix channel count check
lavu : prevent overflow in av_clip_intp2_c
avformat / apngdec : Use 64bit for ret to avoid overflow
avcodec / wavpack : Fix runtime error : left shift of negative value - 5
Avoid a null pointer dereference on oom in the aac encoder .
lagarith : avoid infinite loop in lag_rac_refill ()
ffprobe : free shared writer context on exit
avformat / thp : Check av_get_packet () for failure not only for partial output
vp3 - mt : fix deadlock when first frame is not a keyframe .
avcodec / mpeg4videoenc : Use 64 bit for times in mpeg4_encode_gop_header ()
ac3dec : ensure get_buffer () gets a buffer for the correct number of channels
avformat / movenc : Check packet in mov_write_single_packet () too
avformat / http : fix memory leak in parse_cookie .
avcodec / truemotion2 : Fix passing null pointer to memset ()
aacdec : fix signed overflows in lcg_random ()
lavfi / volume : fix NULL reference in filter_frame ()
v4l2_m2m : Fix integer overflow in timestamp handling
avutil / timecode : Fix fps check
mpegvideo_enc : remove assert that has become obsolete with the new API
avcodec / xan : Improve overlapping check
avutil / mathematics : make av_gcd more robust
avcodec / mjpegdec : Fix runtime error : left shift of negative value - 511
indeo4 : fix out - of - bounds function call .
avcodec / diracdec : Fix Assertion frame -> buf [ 0 ] failed at libavcodec / decode . c : 610
lavc : fix recoded subtitles end .
pmpdec : make i unsigned , avoid undefined behavior of i ++
avcodec / h264_refs : Fix long_idx check
aacdec : Fix an off - by - one overwrite when switching to LTP profile from MAIN .
avformat / oggparseogm : check input size before reading t
avutil / mathematics : Fix division by 0
avformat / nutdec : Check ff_gen_search () for failure
avformat / apngdec : Use 64bit ret to avoid overflow
pthread_frame : don ' t sync items between threads for intra - only codecs .
avcodec / utils : fix memleak on avcodec_open2 () failure
avcodec / wavpack : Change wp_log2 () to unsigned
pngdec : do not release buffer on failure instead report full progress
swresample : fix negative rematrix volumns
lavc / avpacket : Fix undefined behaviour , do not pass a null pointer to memcpy ().
tiny_psnr : fix range calculation for sample size of 32 bits
avcodec_decode_audio : do not trust the channel layout , use the channel count .
mxfdec : Include FF_INPUT_BUFFER_PADDING_SIZE when allocating extradata .
libfaac : Fix assignments in if ()
utils : Do not expand a macro with ' defined ' in it
avcodec / wma : Print more details in case of spectral RLE overflows
lavf / id3v2 : do not export empty fields .
libavformat / flvdec . c : free always the packet after a resync .
dvbsubdec : handle mode change page state
get_bits : make cache unsigned to eliminate undefined signed overflow .
avcodec / snowenc : Clear MMX state after edge drawing and picture encode
flvdec : Fix compiler warning for uninitialized variables
dashdec : Only free url string if being reused
avcodec / ass : check for av_mallocz () failure
ffmpeg : Replace goto redo on decode fail with continue .
lavf / segment : exit immediately in case of invalid stream specifier
avformat / rtmppkt : Fix random crash
avoid infinite loop if pixel format conversion does not exist
avcodec / mpegaudiodec_template : Fix 2 runtime error : signed integer overflow
Provide a fallback version of the libm function trunc
fixing q > 0 . 0 assert failure caused by overflow of variance for b frames
avconv : don ' t pass a bogus parameter to avfilter_graph_create_filter ().
ffmpeg : fix uninitialized variable warning
indeo3 : fix data size check
tta : check for extradata allocation failure in tta demuxer
lavc / avpacket : Fix undefined behaviour , do not pass a null pointer to memcpy ().
avformat / mpegenc : improve field names in underflow error message
vf_delogo : fix an uninitialized read .
avformat / mxfdec : Fix Sign error in mxf_read_primer_pack ()
avcodec / jpeglsdec : Check for end of bitstream in ls_decode_line ()
libopencore - amrdec : allow sampling rates other than 8khz
tiffdec : Prevent illegal memory access caused by recycled pointers .
avcodec / svq3 : use av_fast_padded_malloc ()
lavr : do not try to copy to uninitialized output audio data .
avformat / mov : Check creation_time for overflow
avformat / rmdec : Check for overflow in ff_rm_read_mdpr_codecdata ()
utils / update_stream_timings : fix division by zero
avformat / matroskadec : fix integer overflow
adpcm : Check for channels to be a non - zero integer
mov : fix leaking memory with multiple drefs .
avformat / img2dec : do not rewind custom io buffers
rtmpproto : make condition work in overflow case .
avformat / riffenc : Use size_t for strlen in ff_riff_write_info_tag ()
avcodec / snowdec : Check for remaining bitstream in decode_blocks ()
apetag : fix error handling in ff_ape_parse_tag ()
Fixed a segfault in the DCA decoder with corrupted streams .
avformat / oggparsetheora : zero extradata padding area
avcodec / lagarith : reenable buggy lag_decode_zero_run_line ()
mpegpsenc : Fix integer overflow of the muxrate calculation .
fate / cabac : replace uninitialized bytes by random bytes
avformat / mxfenc : Set color siting to 0 for D10 - MXF
avcodec / ffv1dec : Free tables on init failure
avcodec / mpegaudiodec_template : Fix multiple runtime error : signed integer overflow
avcodec / libtheoraenc : Check for av_malloc failure
avformat / avio : Fix null pointer dereference in case of memleak
avcodec / mjpegdec : Clip DC also on the negative side .
avcodec / a64multienc : use av_frame_ref instead of copying the frame
cook : Make constants passed to AV_BE2NE32C () unsigned to avoid signed overflow .
ffprobe : fix leak in show_packets ()
flvenc : silence bogus warning
ffprobe : set writer context to 0 after allocation
avcodec / ffv1dec : Explicitly check read_quant_table () return value
h264 : check that execute_decode_slices () is not called too many times
sonicdec : fix frame size
adxdec : Fix division by zero
avcodec / ac3enc : fix undefined negative left shift
snow : zero scratchbuf .
avprobe : Unref the packet once it is used
avcodec / tiff : Clear deinvert_buf_size on deallocation
hlsenc : make segment number unsigned
huffyuv : error out on bit overrun .
aacdec : fix " may be used uninitialized " warning
jvdec : check videosize
avformat : Fix Pro - MPEG non - square matrix
asfdec_o : prevent overflow causing seekback
avformat / test / fifo_muxer : add check for FailingMuxerPacketData alloc
avcodec / samidec : check av_strdup () return value
lavf / dnxhd : Autodetect more valid files .
avcodec / lossless_videodsp : add_hfyu_left_prediction_int16_c : fix harmless integer overflow
avformat / dsfdec : set bit_rate , fixes duration estimation
lavf : fix the comparison in an overflow check
avformat / jvdec : clear packet padding after downsize
avcodec / tdsc : use ff_codec_open2_recursive ()
mem : Handle av_reallocp (..., 0 ) properly
proresdec_lgpl : explicitly check coff [ 3 ] against slice_data_size
vp9 : reset segmentation information on context reset .
mxfdec : Fix a potential DoS vector in mxf_read_pixel_layout ()
on2avc : Fix out of array access
avformat / mp3dec : fix msan warning when verifying mpa header
avformat / vqf : check avio_read () s return value more completely
applehttp : Avoid reading uninitialized memory
avcodec / pixlet : fixes integer overflow in read_highpass ()
crash fix by Carl Eugen Hoyos cehoyos ag . or at
avformat / hevc : fix mix of av_malloc () with free ()
movdec : Free the previous extradata
avformat / ape : Use av_malloc_array ()
h264 : fix variable overflow after a few years of video playback
lavu : test for broken binutils on ARM
dcadec : Always initialize return variable
swr / rematrix : fix C99 left shift overflow
doc / examples / decoder_targeted : Limit max pixels for fuzzing
hls : do not access pb -> opaque for custom IO .
h2645_parse : don ' t overread AnnexB NALs within an avc stream
roqaudioenc : Fix crash with very small roq files
libavcodec / qsvenc . c : A warning message when library will work at partial hardware acceleration .
check region depth for validity
h264 : on reference overflow , reset the reference count to 0 , not 1 .
latmdec : Check AudioSpecificConfig length before decoding extradata .
af_amix : prevent memory leak on error path
avutil / opt : fix av_log type
vp56 : check buffer size to fix a potential segfault
lavu / error : fix sign error in av_sterror () error code
lavf / mov : fix crash in mov_read_sidx
avcodec / bsf : fix resource leak in av_bsf_list_parse_str
avcodec / wavpackenc : print channel count in av_log call
avcodec / pngenc : Initialize fctl_chunk to 0
avformat / iff : shrink packets to the initialized data
avcodec / pixlet : check out of bounds pfx value
indeo5 : Fix null pointer dereference
x86 : ac3dsp : Remove 3dnow version of ff_ac3_extract_exponents
vf_mp : fix null ptr deref in case of ENOMEM
avformat / matroskadec : fix off by 1 error in matroska_read_seek ()
avcodec_encode_audio2 : Increase the audio buffer size .
lavf / segment : remove duplicated and inconsistent cleanup code in seg_write_packet ()
hevc : unref the current frame if frame_start () fails
makes really sure that no undefined behavior can happen
checkasm : v210 : Fix array overwrite
avcodec / xwddec : Check bpp more completely
wmalosslessdec : fix mclms_coeffs * array size
avformat / movenc : Do not pass AVCodecParameters in avpriv_request_sample
avresample : prevent theoretical division by zero
avcodec / eamad : silence uninitialized variable warnings
avcodec / wavpack : Fix undefined integer negation
wmadec : avoid infinit loop .
indeo4 : update AVCodecContext width / height on size change
avcodec / hnm4video : change width / height to int
aiffdec : fix division by zero
avformat / matroskadec : use av_realloc_array ()
buffersrc : fix invalid read in uninit if the fifo hasn ' t been allocated
Prevent scalefactors from overflowing .
avcodec / webp : Fix null pointer dereference
mov : detect EOF in mov_read_dref ()
fix sigfpe with intra only encoding
mxfdec : return error if no segments are available in mxf_get_sorted_table_segments
avs : silence " may be used uninitialized " warnings
matroskadec : prevent access of elements after freeing
pthreads : reset got_frames on flush .
avformat / dump : Fix integer overflow in av_dump_format ()
avcodec / shorten : Check k in get_uint ()
golomb : always check for invalid UE golomb codes in get_ue_golomb
avformat / idcin : Use 64bit for ret to avoid overflow
rtsp : Fix compiler warning for uninitialized variable
hwcontext_dxva2 : initialize D3DDISPLAYMODEEX correctly
avcodec / hevc_ps : prevent stale pointer in malloc failure case
remove warnings about uninitialized vars
vorbis : Fix last quarter of CVE - 2011 - 3893
avcodec / cllc : Check prefix
g723_1dec : remove unneeded cliping that leaked in from merge from libav
lavf / audiointerleave : check for allocation failure
avcodec / msrle : check return code for success before use
libavformat / mov : Fix memory leak , need to free the chapter tracks array
wtvdec : fix integer overflow resulting in errors with large files
avformat / 4xm : shrink packet if it was only partially initialized
avformat / utils : check for malloc failure
lavf : avformat_seek_file (): validate stream_index .
ffmpeg : dont call exit_program () from a signal hander
avpriv_mpeg4audio_get_config : check init_get_bits () return code .
Prevent calling init_vlc () with invalid parameters in motionpixels decoder .
avcodec / vp8 : Fix null pointer dereference in ff_vp8_decode_free ()
avcodec / fmvc : Fix off by 1 error
imgconvert - test : add avg bits per pixel
vorbisdec : do not leak the first frame .
avutil / frame : Copy size = 0 side data in ff_init_buffer_info ()
aacenc_ltp : fix assertion
pmpdec : Check for zero audio packets .
Fix a mem leak in vc1_decode_frame ().
avcodec_encode_audio (): fix invalid free
ffmpeg : disable autoinsertion for - async on the output side of the filtergraph .
avcodec / jpeg2000dec : zero Jpeg2000QuantStyle structure before use in get_qcd ()
uninitialized var ...
lavfi / fade : Do not overread input buffer .
mxf_set_audio_pts : fix division by 0
hdsenc : Fix an off by one error in an array size check
lavfi / showwaves : do not set on outlink -> out_buf the sent video frame
lavf : Make sure avg_frame_rate can be calculated without integer overflow
fix exploitable buffer overflow
avcodec / mpeg12dec : check for overread in mpeg1_fast_decode_block_inter ()
use filter name when graph parser add filters
avcodec / msmpeg4dec : Check for input end in msmpeg4v34_decode_mb ()
shorten : Fix out of bound writes in fix_bitshift ()
postproc / postprocess : Check for av_malloc () failure
avcodec / webvttdec : Fix uninitialized use of variable " again "
avcodec / zmbv : Check that the buffer is large enough for mvec
sbgdec : dont set slide to a uninitialized value
lavf / ftp : fix possible crash
ff_gen_search : make step 64it to prevent hypothetical integer overflow
swresample / swresample : Use av_mallocz_array ()
tm2 : Check remaining size before init_get_bits ()
ffmpeg : Check for subtitle_out malloc failure
avcodec / wmalosslessdec : deallocate uninitialized frame on decode_tilehdr () failure
xwddec : prevent overflow of lsize * avctx -> height
avcodec / sheervideo : Check input buffer size before allocating and decoding
atrac3 : Fix crash in tonal component decoding .
id3v2 : allocate large enough buffer
avcodec / snowdec : Fix runtime error : left shift of negative value - 1
lavf / rtsp : Fix a crash with the RTSP muxer .
mpegvideo : initialize videodsp with correct pixel depth
timecode : fix typo
lavfi / lut3d : avoid overread .
avformat / rtsp : Fix potential pointer overflow in sdp_probe ()
avcodec / mpeg4videoenc : Use 64 bit for times in mpeg4_encode_gop_header ()
lagarith : check count before writing zeros .
af_aresample : fix leak on alloc failure
libavcodec / wmalosslessdec : add missing emms
decode : Initialize ret before using it
vc1dec : prevent memory leak in error path
ffmpeg : fix a crash with complex filters when pix_fmt is not specified
oggenc : add missing initializer braces to shut up gcc warning .
avcodec / h264_mp4toannexb_bsf : Reorder operations in nal_size check
avformat / nutdec : Check X in 2nd branch of index reading
lavc : add 422 / 444 YUV with alpha to align_dimensions ()
mpc7 : check for allocation failure
msmpeg4 : Don ' t set up run - level info for level 0 .
matroskadec : avoid potential mem leak
avcodec / libstagefright : Check for pthread_create () failure
ff_msmpeg4_decode_init () calls ff_h263_decode_init () which calls
avformat / msf : Check channels when reading the header
ffserver : Clear avio context after closing it
ffmpeg : fix uninitialized variable warning
avcodec / dca_lbr : Fix " warning : missing braces around initializer "
vf_scale : don ' t leak SWS context .
avcodec / hevc : make * ps_id unsigned
mpegenc : simplify muxrate calculation
check udp_read_packet return value not to access uninitialized memory
avcodec / mlpdsp : Fix signed integer overflow , 2nd try
avidec : use actually read size instead of requested size
Values > 4 for lowres corrupt output .
yuv2rgb : fix integer overflow in fill_table
av_image_copy : check for av_image_get_linesize () failure .
avcodec / dirac_vlc : Fix invalid shift in ff_dirac_golomb_read_32bit ()
avcodec / hevc_ps : Check return code from pps_range_extensions ()
put_bits : fix invalid shift by 32 in flush_put_bits ()
avpacket : fix size check in packet_alloc
wmalosslessdec : Fix reading too many bits in decode_channel_residues ()
mpegvideo_enc : initialize the encoding context
libx264 : fix double free
avformat / mux : Apply auto bsfs in av_write_frame () too
xwma : check bytes_per_sample , fix division by 0 .
avcodec / dvdsubdec : Fix off - by - one error
avcodec / flacdec : Fix undefined shift in decode_subframe ()
avcodec / exr : fix null pointer dereference
indeo3 : error out if no motion vector is set .
lavfi / hue : fix crash when resetting the saturation expression
avplay : reset rdft when closing stream .
Fixed a possible bug , checked against the assembly .
dsicin : Add some basic sanity checks for fields read from the file
avcodec / cavsdec : Fix undefined behavior from integer overflow
vp9_raw_reorder_bsf : Remove a redundant allocation
avformat / vqf : Use 64bit for ret to avoid overflow
avcodec / jpeg2000dec : Zero code and quant style
avcodec / libfdk - aacdec : Correct buffer_size parameter
pngdec : Fix interlaced PAETH prediction
oggparseskeleton : Check the overall start time before using it .
avcodec / mpeg_er : Clear mcsel in mpeg_er_decode_mb ()
shorten : dont leave invalid channel counts in the context .
Check for out of bound reads in the Electronic Arts CMV decoder .
targa : prevent integer overflow in bufsize check .
hlsproto : Properly close avio buffer in case of error
prores : Evaluate all the quantizers
apetag : propagate errors .
dca : Fix uninitialized variable warnings .
matroskadec : don ' t warn about unknown spherical medata when none is present
loco : silence warning : decoded may be used uninitialized in this function
avformat / segafilm : check 2nd tag
rv20 : prevent calling ff_h263_decode_mba () with unset height / width
cafdec : fix overflow checking in read_header ()
avcodec / utils : Check close before calling it
sgidec : safer check for buffer overflow
asf : reset side data elements on packet copy .
pcmdec : check that channels is valid .
sol : return error if av_get_packet () fails .
iff : validate CMAP palette size
examples / avio_reading : fix null dereference on error
rdt : Check the return value of avformat_open
ape : Use the proper variable type
avformat / thp : check av_get_packet () for failure
swscale : fix 16 - bit horizontal scaling underflow .
lavf / img2dec : Initialize a stack variable .
avformat / lrcdec : Fix memory leak in lrc_read_header ()
audemux : Add a sanity check for the number of channels
dv : Initialize encoder tables during encoder init .
avconv : free packet in write_frame () when discarding due to frame number limit
avcodec / truemotion2rt : also export key frame info
dvbsubdec : add missing break
avcodec / cinepakenc : Zero AVPictures , silence " may be used uninitialized " warnings
flvdec : Inject in stream extradata , into the stream .
avformat / mpegts : reset last_ver on corrupted packets
avcodec / mlpdec : Fix runtime error : left shift of negative value - 22
avformat / sapenc : Use av_mallocz_array ()
aaccoder : prevent crash of anmr coder
avcodec / wavpack : Treat the first block coding too many channels as an error
g723_1 : fix off - by - one error in normalize_bits ()
audemux : Fix potential integer overflow leading to a division by 0
wavpack : Fix an integer overflow
vaapi_encode : Discard output buffer if picture submission fails
ffprobe : check av_asprintf () for failure
rtmpproto : Lengthen the filename buffer when receiving streams
avformat / movenc : Write version 2 of audio atom if channels is not known
avformat / oggdec : check for av_packet_new_side_data () failure
libshine : fix support for shine 3 . 0
svq1dec : Fix overread on very small input
mov : Fix negative size calculation in mov_read_default ().
avformat / bintext : protect against potential overflow of chars_per_frame
avcodec / snow : Initialize spatial_decomposition_count to a valid value
ipmovie : do not read audio packets before the codec is known
avcodec / mpegaudiodec_template : Correct return code on id3 tag discarding
asink_nullsink : plug a memory leak .
lavr : check that current_buffer is not NULL before using it
cpu detection : avoid a signed overflow
lavfi / separatefields : fix frame leak
escape124 : reject codebook size 0
matroskadec : fix potential mem leak
avcodec / indeo4 : Check remaining data in Pic hdr extension parsing code
ac3_parser : add required padding for GetBitContext buffer
lavf / hlsenc : fix basename size computation off - by - one bug
avcodec / ffv1dec : Fix integer overflow in read_quant_table ()
avformat / mov : fix integer overflow in mov_read_udta_string ()
atrac3 : Fix crash in tonal component decoding .
avcodec / jpeg2000 : Check sum of sizes of band -> prec before allocating
indeo : Reject impossible FRAMETYPE_NULL
lagarith : pad RGB buffer by 1 byte .
bethsoftvid : check return value of av_packet_new_side_data ()
avformat / movenc : Fix use of uninitialized variable ( ret )
apetag : do not leak memory if avio_read () fails
avcodec / utils : Fix potential overflow in overallocation code
rtmp : Clean up properly if the handshake failed
avformat / movenc : avoid using uninitialized display_matrix_size
avformat / oggparseopus : Fix Undefined behavior in oggparseopus . c and libavformat / utils . c
vp56 : alpha_offset is uninitialized on purpose
swfdec : check lens validity
avformat / oggdec : Fix integer overflow with invalid pts
af_join : do not leak input frames .
avcodec / flacenc : Fix Invalid Rice order
mpegpsmux : Fix integer overflow with huge VBV sizes .
jvdec : check that the video_size fits in the packet .
vc1dec : prevent v_edge_pos from becoming negative .
doc / examples / filtering_audio : init packet0 . data
avformat / rtpenc : check av_packet_get_side_data () return , fix null ptr dereference
avcodec / ac3dec_fixed : Fix integer overflow in scale_coefs ()
lavf / vf_framerate : Fix frame leak when increasing framerate .
avcodec / dirac_arith : fix integer overflow
crypto : Use av_freep instead of av_free
libx264 : Define X264_API_IMPORTS on MSVC / ICL
idroqdec : fix leaking pkt on failure
mjpegenc : Remove duplicate initializer
udp : do not call pthread_ { mutex , cond } _destroy when not initialized .
avcodec / h264 : Clear delayed_pic on deallocation
avcodec / bmp : fix integer overflow in checking header sizes
vf_hqdn3d : make config_props work properly when called multiple times .
lavc / options : fix shallow copy context
rtmpproto : Validate the embedded flv packet size before copying
avcodec / diracdec : Check perspective_exp and zrs_exp .
ffprobe : fix use of uninitialized pointer in av_strtok ()
diracdec : Test mctmp and mcscratch for malloc failure .
avcodec / mpegvideo_enc : check that max_b_frames is not negative
avcodec / vp3 : check current_frame before accessing it
mpegts : Fix potential memory leak in mpegts_read_packet ()
fixed long standing off - by - one bug ( fixes playback on ppc )
libx265 : Fix use of uninitialized input picture
wmaprodec : check num_sfb for validity . Fix out of array accesses
avformat / vqf : check number of channels before use .
shorten : allocate space for padding
rtsp : Fix a crash with the RTSP muxer
lavf / utils : Do not force chapter end time before chapter start .
avcodec / aacdec_fixed : Fix integer overflow in apply_dependent_coupling_fixed ()
lavf / utils : ignore subtitle timestamps in timings calculation
pthread_frame : Unreference hw_frames_ctx on per - thread codec contexts
check for sane values in ogg_get_length (), fix lol - ffplay . ogm fuzz test
af_asyncts : avoid overflow in out_size with large delta values
mp3enc : fix CODEC_ID / CodecID that leaked in from older patch
matroskadec : use uint64_t instead of int for index_scale
ac3enc : simplify sym_quant ()
riff : fix division by zero for G726
Free the RTSPStreams in ff_rtsp_close_streams
avcodec / interplayvideo : Fix timeout from lack of bitstream end check
yop : check return value of avformat_new_stream ()
dds : disable palette flag for compressed images
lavc / audiotoolboxdec : fix memory leak
libavutil / hwcontext_dxva2 : Add check for possible errors from GetAdapterDisplayModeEx
rmdec : fix crash at end of file
H264 : hotfix for speedloss on frame threading and h264 files with slices .
avformat / mov : zero initialize codec_name in mov_parse_stsd_video ()
avformat / ipmovie : Check OPCODE_CREATE_TIMER size
Fix a crash on oom when decoding hevc .
avcodec / x86 / dsputil_init : only use xvid idct for lowres = 0
codec_desc : mark fraps as an intra - only codec .
avformat / flvdec : initialize context before reading from it
avutil / opt : initialize ret
rv30 : check block type validity
Arrays where one element too small , fixes CID114 .
jvdec : Return EOF on end of file
swscale - example : Don ' t return random value from main ().
avcodec / mpeg12dec : Fix runtime error : left shift of negative value - 1
avformat / mpeg : zero initialize idx_pkt
mpeg12 : terminate mpeg2_video_profiles arrays
pan : fix uninitialized channel_id variable .
lavfi / nnedi : Fix a memleak .
rv34 : Avoid NULL dereference on corrupted bitstream
rtpdec : Limit writing to the buffer size
avcodec / diracdec : Fix integer overflow in divide3 ()
avcodec / tiff : remove byte based bpp special case
r3d : check that sampling rate is non negative .
avcodec / pcm : Fix overflow in bitrate computation
flvdec : skip duplicate indexes
avcodec / mlpdec : Check that there is enough data for headers
avcodec / utils : Align YUV411 by as much as the other YUV variants
libxvid_rc : fix leaks in ff_xvid_rate_control_init ()
avcodec / imgconvert : fix possible null pointer dereference
mxfdec : Fix files with essence containers larger than 2 GiB .
lpc : use default number of passes when lpc_passes is invalid
rtspdec : read_line : fix use of uninitialized byte
avcodec / vqavideo : use av_mallocz () for decode_buffer ()
dpx : Make start offset unsigned
allcodecs : move libaacplus down .
tiff : check bppcount
avcodec / nvenc : fix potantially uninitialized free
avformat / takdec : zero padding area before use
mov : reset dref_count on realloc to keep values consistent .
electronicarts : Add more sanity checking for the number of channels
avcodec / wnv1 : More strict buffer size check
txddemux : Limit allocated packets to filesize .
http : Make sure proxyauth is initialized
vorbisdec : avoid invalid memory access
imc : Catch a division by zero
lavc / opusdec : Fix a memleak when reading invalid files .
avcodec / jpeglsdec : Check ilv for being a supported value
mov_probe : fix integer overflows
avformat / dashdec : Fix memleak when resolve_content_path
avidec : Free packet if dv_produce_packet fails .
dsicinav : Check for out of bounds writes
xwddec : prevent overflow of lsize * avctx -> height
avcodec / vda_h264 : fix null pointer dereference
perform sanity check on number of channels in electronicarts demuxer
avcodec / avpacket : zero memory in av_packet_new_side_data ()
lavfi / gradfun : avoid use of uninitialized variable .
Avoid a null pointer dereference on clean - up after oom in ac3 encoder .
cbs_h2645 : Fix memory leak on when reading SEI fails
avcodec / hevc_ps : Fix c ? _qp_offset_list size
avcodec / gdv : Check available space before reading palette
mem : Make av_strdup allocate using av_realloc
avformat / adxdec : dont return random data on EOF
audio_mix : initialize the data pointers to NULL
yuv2rgb : fix integer overflow in fill_gv_table ()
avcodec / avpacket : Fix memory allocation failure check
lavf / matroskaenc : fix uninitialized read
diracdec : clear slice_params_num_buf on allocation failure
smush : check audio packet size
avcodec / mpegvideo_enc : Fix leak on bitstream buffer reallocation
mem : Consistently return NULL for av_malloc ( 0 )
avidec : add missing initializer braces to shut up gcc warning .
avcodec / vdpau_hevc : Fix potential out - of - bounds write
seek back at the end of file after updating header
avcodec / mlpdec : Do not leave a invalid num_primitive_matrices in the context
Fix a crash when reading gray pam files .
avcodec / magicyuv : set correct size of last slice for each plane
avformat / movenc : Fix potential leak of sgpd_entries array .
avcodec / dnxhddec : Do not overwrite colorspace if the container has set it .
lavc / videotoolboxenc : fix open videotoolbox bug on iOS8 . 4
movenc : Check that frag_info entries exist in mov_write_sidx_tag
tcp : Check the return value from getsockopt
lzw ( gif ): Fix overread
decode : avoid leaks on failure in ff_get_buffer ()
targa : prevent integer overflow in bufsize check .
oops , potential overflow on really large blocks
matroska : Clear prev_pkt between seeks .
avcodec / alsdec : Check for overread
kill uninitialised variable warning in ac3_probe ()
avcodec / cbs_jpeg : Check length for SOS
lavc / mjpegdec : Do not overread too short JFIF tags .
h264 : fix () placement
aacenc : Prevent premature termination of the two loop search .
avformat / movenc : Check that frame_types other than EAC3_FRAME_TYPE_INDEPENDENT have a supported substream id
avformat / avio : fix memory leak in url_find_protocol
flashsv : make sure data for zlib priming is available
motionpixels : check extradata size
avformat / oggparseogm : Check available data before reading global header
avcodec / cavsdec : Check esc_code
lavf / img2dec : fix out - of - range check in find_image_range ()
avcodec / h264 : Only reinit quant tables if a new PPS is allowed
xbmdec : fix off by one error in scanf ()
dfa : Put our pointer check back .
rawdec : don ' t leak option strings .
avformat / hlsenc : Free context after hls_append_segment
Check for out of bound writes when building tree in bink decoder .
ffmpeg : Fix crash with ost -> last_frame allocation failure
avformat / matroskaenc : add missing allocation failure checks for stream durations
yopdec : check frame oddness to be within supported limits
fix decoding of coupling leak info for normal AC - 3
avcodec / imc : dont read bits beyond the end
sanity check whether dimensions are non - null
avoid deprecation warnings for AVFrac from avformat . h
qsvdec : Drop stray extra braces around initializer
lavc / cdxl : clear palette before reading it
avformat / apngenc : use av_packet_alloc ()
avcodec / iff : warn about truncated input to decode_byterun () and clear remaining output
avcodec / mpegvideo_enc : Check for integer overflow in ff_mpv_reallocate_putbitbuffer ()
avs : fix infinite loop on end - of - stream .
lavc / jpeg2000dec : Fix jp2 inner atom size used for overread checks .
avformat / tls_schannel : Fix use of uninitialized variable
avcodec / rv40 : Test remaining space in loop of get_dimension ()
lavf : fix invalid reads in avformat_find_stream_info ()
mpegvideo : unref buffers in ff_mpeg_unref_picture on frame size changes
ffmpeg : Fix null pointer dereference in do_video_out ()
eacmv : check for out of bound reads
mpeg12enc : Don ' t set up run - level info for level 0 .
avcodec / h264_cavlc : Fix undefined behavior on qscale overflow
avformat / utils : Check AVFormatContext -> duration in compute_chapters_end () before use
ac3enc_template : silence may be used uninitialized in this function warnings
avcodec / libtheoraenc : Check for stats allocation failure
lavf / tee : fix leak of select option .
softfloat : handle - INT_MAX correctly
vf_paletteuse : Don ' t free the second frame from ff_framesync_dualinput_get_writable on error
Avoid a null pointer dereference on oom when decoding vc1 .
sws : Fix warning : ‘ abase ’ may be used uninitialized in this function
cbs_h264 : Fix memory leak in error case
cook : fix use of uninitialized variable
h264 : reset current_slice on context reinit
avformat / rmdec : Fix Packet memleak at close ()
threads : fix a potential race spotted by helgrind .
utils : fix avcodec_flush_buffers pre - reference counting compatibility
avcodec / utils : Clear dimensions in ff_get_buffer () on failure
vf_drawtext : fix memory leak of glyph data .
avcodec / hevc : fix nb_sh / nb_sps check
avformat / utils : fix memory leak in avformat_free_context
sanm : fix undefined behaviour on big - endian .
amrwbdec : Decode the fr_quality bit properly
avcodec / mjpegdec : Add some sanity checks to ljpeg_decode_rgb_scan ()
avcodec / ituh263dec : Check for the bitstream end in ff_h263_decode_mb ()
avformat / isom : zero MPEG4AudioConfig to avoid leaving fields uninitialized
mmvideo : fix uninitialized variable use in mm_decode_intra
rtsp : Set the real_setup pointer properly , avoid out of bounds memory accesses
mp3on4 : fix null pointer dereference
avcodec / utvideodec : Fix bytes left check in decode_frame ()
avcodec / mpeg4video_parser : Check for extradata parsing failure
vc1_parser : fix NULL pointer dereference
ividsp : zero pitch so as not to overread
avformat / utils : Do not select audio streams with unknown sample rate in av_find_best_stream ()
avformat / rmdec : check that sub_packet_size fit in the packet for DEINT_ID_GENR
Fix a leak in the AAC encoder
avcodec / mpeg4videodec : use av_fast_padded_malloc () for bitstream buffer
avcodec / huffyuvdec : do not leave alpha values uninitialized
fraps : check for overread .
ape : stop reading after the last frame has been read
movtextenc : Remove dangling reference to movtext . h
avformat / utils : fix a codecpar non use
mov : fix self referencing timecode tracks
avconv : fix off by one check in complex_filter
avformat / hls : do not limit manifest lines to 1024 chars
lavc / ass_split : check for NULL pointer in ff_ass_split_override_codes ().
imc : Catch a division by zero
avcodec / diracdec : Fix off by 1 error in quant check
supporting rare overflow mess even with error_resilience >= 0
ffv1 : Make sure at least one slice context is initialized
diracdec : Use only one frame per reference
avcodec / sgidec : fix count check
lavf : prevent crash in av_open_input_file () if ap == NULL .
segafilm : Check for memory allocation failures in segafilm demuxer .
avcodec / eamad : use av_freep (), do not leave stale pointers in memory
avcodec / mjpegdec : Check for end for both bytes in unescaping
h264 : Make sure reinit failures mark the context as not initialized
avformat / rmdec : Fix use of uninitialized variable
avcodec / h264_refs : Change default case to av_assert0 () to suppress warning
libfdk - aacdec : Fix a boundary check
vsrc_movie : fix leak in request_frame ()
vp8 : prevent read from uninitialized memory in decode_mvs
libavformat / cdxl : fix duration in case of overflow
avformat / mp3dec : Check for avcodec_alloc_context3 () failure
matroska : set " done " only during resync fail .
mpegts : Make sure we don ' t return uninitialized packets
avcodec / cfhd : Fix invalid left shift of negative value
dont reduce the min_distance of AVIndexEntry
truemotion2 : clear the token array if its initialization fails .
gifdec : fix invalid write in gif_copy_img_rect
avcodec / mpegaudioenc_template : dont crash on bitrate = 0
opus_pvq : remove unneeded assert
g723_1 : fix overflow in square_root ()
ffmpeg_cleanup : fix crash with unrecognized codec
elsdec : Replace EOVERFLOW with INVALIDDATA
vfwcap : Add support for UYVY pixel format .
avcodec / nuv : zero buffer padding
avcodec / shorten : if allocation fails reset max_frame_size
smc : fix the bounds check
vf_overlay : ensure the overlay frame does not get leaked .
avcodec / webp : Fixes null pointer dereference
avcodec / vc1dec : fix null pointer dereference
mxfdec : free descriptor extradata in . read_close ()
oggparseogm : check timing variables
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
mm : Skip unexpected audio packets
hls : fix leaking avio_opts on hls_read_header error
mjpegdec : rgb mode is specific for ljpeg , disable it for others .
avcodec / mpegvideoenc : fix undefined negative left shift
crc : fix signed overflow
avcodec / vp56 : Reset have_undamaged_frame on resolution changes
lavc / h264 : move ff_init_cabac_states () from decode_slice () to ff_h264_decode_init ().
doc / examples / decoder_targeted : Disable error concealment after 20 frames
mov : fix global unicode convertion array overflow .
imc : add required padding for GetBitContext buffer
flashsv : check for keyframe before using differential coding
hlsproto : fix integer overflow
avcodec / pafvideo : Fix assertion failure
mxfdec : fix potential integer overflow in mxf_compute_sample_count ()
vsrc_movie : fix leak in request_frame ()
check for error
a64multienc : fix " warning : buf may be used uninitialized in this function "
avcodec / dnxhddec : reset cur_field for non interlaced frames
lavfi / ocv : fix crash with no filter name specified .
h264 : only assign H264Picture :: mbaff for first slice .
avformat / oggenc : check for stream private data in ogg_free ()
lavfi / subtitles : 10l close codec before format .
avcodec / hevcdsp_template . c : Fix undefined shift in FUNC ( dequant )
avcodec / aacdec_fixed : Fix runtime error : left shift of negative value - 1297616
cmdutils_opencl : Fix read of uninitialized pointer
lavf : revert commit bb99ae3ae924c942a634bec7711ec7ee11c38eb9
check for qscale == 0 ( fixes 1 / 0 on one corrupted stream )
avcodec / hevc_parser : check for av_mallocz () failure
swscale : Do not expand a macro with ' defined ' in it
avcodec / takdec : Fix runtime error : left shift of negative value - 42
smvjpegdec : make sure cur_frame is not negative
movenc : fixes a questionable valgrind uninitialized value warning
avcodec / dxva2_h264 : Fix " may be used uninitialized " warnings
swr : fix " may be used uninitialized in this function " warnings
mpeg12 : terminate mpeg2_video_profiles arrays
opus_pvq : prevent division by 0
msmpeg4v1 : fix undefined behaviour in msmpeg4_decode_picture_header ()
avcodec / shorten : clear bitstream buffer
avcodec / webp : Add missing input padding
vsrc_testsrc : fix off - by - one logic when detecting the EOF time in request_frame ()
avformat / matroskadec : fix resource leak
aacdec : free frame buffer if no audio was decoded
avformat / sbgdec : Check alloc_array_elem () return value
h264 : don ' t re - call ff_h264_direct_ref_list_init () w / frame - mt .
checkasm : Give macro a body to avoid potential unexpected syntax issues
libavcodec : Don ' t do av_free ( av_malloc ( 0 )) for bitstream filters
avidec : update size when packet is shrunk
png : Use av_freep () instead of av_free ()
vc1 : properly zero coded_block [] edges on new slice entry .
avcodec / wavpackenc : fix uninitialized ret
mov : Free an earlier allocated array if allocating a new one
qcelpdata : correct start bit for cbgain [ 0 ] in qcelp_rate_octave_bitmap []
vf_overlay_opencl : Don ' t leak output frame on error
avcodec / tiff : reset sampling [] if its invalid
avcodec / wavpack : Fix invalid shift and integer overflow
VP8 : add missing free
cfhd : fix off - by - one error in level check
libschroedingerdec : don ' t produce empty frames
avcodec / pictordec : Check plane value before doing value / mask computations
g723 . 1 : fix addition overflow
asf : only set index_read if the index contained entries .
mpegmux : add stuffing to avoid incomplete PCM frames
avcodec / aac_adtstoasc_bsf : Fix memory leak
avcodec / vp9 : mark frame as finished on decode_tiles () failure
avcodec / h264 : Be more strict on rejecting pps_id changes
mmvideo : restore initial y value .
avcodec / dxv : Check for end of input in dxv_decompress_dxt5 ()
avformat / mux : Check that deinit is set before calling it
riff : don ' t overwrite bps from WAVEFORMATEX if EXTENSIBLE doesn ' t contain that data .
ffmpeg : add image size check to codec_get_buffer ()
qsvdec : Fix memory leak on error
ra144enc : fix two " may be used uninitialized in this function " warnings
avformat / asfdec_f : Fix memleak
avcodec / wmaprodec : check that there are input bits left in decode_subframe_length ()
avcodec / ratecontrol : silence " warning : expected_bits may be used uninitialized in this function "
avformat / utils : Check cur_dts in update_initial_timestamps () more
applehttp : Change the variable for stream position in seconds into int64_t
motionpixels : clip VLC codes .
AMV : disable DR1 and don ' t override EMU_EDGE
h264 : cap max has_b_frames at MAX_DELAYED_PIC_COUNT - 1 .
ac3 : validate end in ff_ac3_bit_alloc_calc_mask
smacker : Avoid integer overflow when allocating packets
doc / examples : Handle new types in avio_list_dir
matroskadec : check h in generic rm packet shuffler
avformat / avidec : set average frame rate from header
cavsdec : check dimensions being valid .
wtv : Make WTV_SECTOR_BITS a 64 bit constant
fixed double free
doc / examples / transcoding : check encoder before using it
binkaudio : prevent extradata overread
lavf / find_stream_info : make sure we decode at least one frame for DTS
avcodec / mpegaudioenc_template : Fix integer overflow
avcodec / rangecoder : Fix range coder corner case handling
av_free () -> av_freep (), patch by Reimar Doffinger , see discussion in
lavd / xv : free graphics context
avformat / asfenc : Check for index_ptr allocation failure
af_amix : avoid spurious EAGAIN .
ac3enc : fix ' warning : block0 may be used uninitialized in this function '
avcodec / hevc : check slice_header_extension data length
matroskadec : fix NULL pointer dereference in webm_dash_manifest_read_header
h264 : return the consumed amountg in case of Q264
avcodec / exr : fix undefined shift in pxr24_uncompress ()
vp3 : Copy all 3 frames for thread updates .
libxvid : Clear extradata pointer when freeing it .
flacdec : fix buffer size checking in get_metadata_size ()
avcodec / rangecoder : Test for invalid corner case
avcodec / mjpegdec : sanity check bits
xmv : Fix integer overflow
avcodec / mpeg4videodec : update bug workaround related variables like they where before
incorrect memcpy size ; adobriyan @ gmail . com
initialize mmcpos , found by guenther
oops , bounds_check_with_label is already proto ' d in sys / disklabel . h
cast vararg sentinel value to pointer
Return early from NVS command submission if no response is required
Adds _PATH_RSH for rsh - using rcmd ()
plug a mem leak
Avoid an assertion failure in elf32_arm_size_dynamic_sections () when
careful with mmap off
rewrite construct that made GCC 2 . 8 do an implicit memset before libc
uhci : alloc can ' t fail , drop check .
trace : break circular dependency in event - internal . h
parallels : wrong call to bdrv_truncate
qcow2 : Assert against currently impossible overflow
print meaningful error message in case of -- disable - vhost - net
vhost : check for vhost_ops before using .
qed : Refuse to create images on block devices
qapi : Fix memory leak
qemu - sockets : do not test path with access () before unlinking
tests : Run the luks tests in test - crypto - block only if encryption is available
usb - serial : Fail instead of crash when chardev is missing
pseries : Don ' t allow TCE ( iommu ) tables to be registered with duplicate LIOBNs
input : add sanity check
block : Plug memory leak on brv_open_image () error path
tests : test - qmp - commands : Fix double free
block : Detect multiplication overflow in bdrv_getlength
usb - bot : Drop not needed " allow_hotplug = 0 "
ide : fix halted IO segfault at reset
tests : fix virtio - scsi - test leak
vvfat : initialize memory after allocating it
qemu - img : Fix insignificant memleak
acl : Fix acl_remove not to mess up the ACL
exec : fix TB invalidation after breakpoint insertion / deletion
qdev : fix OVERFLOW_BEFORE_WIDEN
target - i386 : Make x86 mfence and lfence illegal without SSE2
sparc / leon3 . c : fix memory leak
s390x / skeys : Fix instance and class size
qemu - char : fix qemu_chr_fe_get_msgfd ()
hw / ppc / e500 . c : Fix memory leak
e1000e : Fix build with gcc 4 . 6 . 3 and ust tracing
pulse - audio : fix bug on updating rpos
ehci : clear suspend bit on detach
cirrus : Fix host CPU blits
hostmem - file : plug a small leak
QemuOpts : check NULL input for qemu_opts_del
vhost : migration blocker only if shared log is used
block / iscsi : fix uninitialized variable
bootdevice : add check in restore_boot_order ()
vmdk : Fix integer overflow in offset calculation
net : Avoid NULL function pointer dereference on cleanup
l2tpv3 : fix possible double free
target - s390x : Only access allocated storage keys
hw / xen_disk : aio_inflight not released in handling ioreq when nr_segments == 0
s390x : fix KVM target
loader : fix potential memory leak
esp : cancel current request only if some request is in flight
qemu - img : Plug memory leak on block option help error path
e1000e : Fix PBACLR implementation
ide : remove undefined behavior in ide - test
block / raw - posix : Don ' t think / dev / fd /< NN > is a floppy drive .
hw / arm / highbank : Fix resource leak and wrong image loading
lm32 : avoid buffer overrun
linux - user / sh4 : fix incorrect memory write
macio : set res_count value to 0 after non - block ATAPI DMA transfers
qemu - img : mandate argument to ' qemu - img check -- repair '
sdhci : don ' t raise a command index error for an unexpected response
blockdev : use drained_begin / end for qmp_block_resize
qcow2 : Avoid integer overflow in get_refcount ( CVE - 2014 - 0143 )
vnc : fix a memory leak in threaded vnc server
hw / ptimer : Do not artificially limit timers when using icount
fix off - by - one error in pci_piix3_xen_ide_unplug
vhost - user : unregister slave req handler at cleanup time
io : fix mem leak in websock error path
qom : Fix crash with qom - list and link properties
qcow2 : fix null pointer dereference
hw / 9pfs : Be robust against paths without FS_IOC_GETVERSION
linux - user : make bogus negative iovec lengths fail EINVAL
init_paths : fix minor memory leak
sparc32 fix np dereference in do_unassigned_access
vhost - user : disable chardev handlers on close
pci_bridge : use parent bus ' s address space
block : Simplify usb_msd_initfn () test for " can read bdrv key "
block / vmdk : Fix the endian problem of buf_len and lba
qemu - socket : don ' t leak opts on error
virtio : Remove malloc failure checks ( Jan Kiszka )
spapr_vscsi : fix build error introduced by f19661c8
Avoid a build warning .
qcow2 : Point to amend function in check
block / qapi : Plug memory leak in dump_qobject () case QTYPE_QERROR
block : fix qcow2_co_flush deadlock
clang : Fix warning reg . expansion to ' defined '
target - ppc : free a tcg temp variable
qcow2 : Fix corruption after error in update_refcount
target - alpha : Suppress underflow from CVTTQ if DNZ
isapc : Fix irq routing
hw / openrisc : Avoid undefined shift in openrisc_pic_cpu_handler ()
qcow2 : Remove abort on free_clusters failure
ide : Correct improper smart self test counter reset in ide core .
tcg - sparc : Dump illegal opode contents
hmp : fix qemu crash due to ioapic state dump w / split irqchip
exec - all . h : Make MAX_OP_PER_INSTR large enough for target - arm ' s uses
fsdev : fix memory leak in main ()
ivshmem : remove redundant assignment , fix crash with msi = off
block : Fix harmless off - by - one in bdrv_aligned_preadv ()
replay : Fix dangling location bug in replay_configure ()
watchdog : fix deadlock with - watchdog - action pause
ahci : clear aiocb in ncq_cb
scsi - disk : change disk serial length from 20 to 36
char : don ' t silently skip tn3270 protocol init when TLS is enabled
qcow2 : Use BB for resizing in qcow2_amend_options ()
rdma : memory leak RDMAContext :: host
sdl : remove NULL check , g_malloc0 can ' t fail
target / ppc : Fix setting of cpu -> compat_pvr on incoming migration
hw / arm / digic : Mark device with user_creatable = false
target - i386 : key sfence availability on CPUID_SSE , not CPUID_SSE2
qga : don ' t fail if mount doesn ' t have slave devices
pseries : Check we have a chardev in spapr_vty_init ()
hmp : migrate command ( without - d ) now blocks correctly
linux - user : fix do_rt_sigreturn on m68k linux userspace emulation
linux - user : Handle brk () attempts with very large sizes
vvfat : Fix segfault on write to read - only disk
usb / ohci : Fix crash with when specifying too many num - ports
ppc : avoid buffer overrun : use pstrcpy , not strncpy
block : record new size in bdrv_dirty_bitmap_truncate
keep the PID file locked for the lifetime of the process
net : avoid infinite loop when receiving packets ( CVE - 2015 - 5278 )
spapr_rng : disable hotpluggability
qxl : switch display on entering VGA
vfio / common : cleanup in vfio_region_finalize
qemu - io : Fix memory leak in ' aio_write - z '
hw / i386 / acpi_build . c : Avoid shifting left into sign bit
cuda . c : return error for unknown commands
char : remove use - after - free on win - stdio
microblaze : boot : Don ' t hack the elf entry point
usb - ohci : Fix memory leak for ohci timer
linux - user / signal . c : Correct error path for AArch64 do_rt_sigreturn
qemu - ga : ga_open_pidfile (): use qemu_open ()
qemu - thread : do not use PTHREAD_MUTEX_ERRORCHECK
ppc : fix monitor access to CR
i8254 : fix out - of - bounds memory access in pit_ioport_read ()
replay : exception replay fix
target / loongarch : Clean up tlb when cpu reset
esp : delay Transfer Information command if dma is not enabled
hw / display / cg3 . c : Fix memory leak
vfio - pci : Fix missing unparent of dynamically allocated MemoryRegion
linux - user : return EINVAL from prctl ( PR_ * _SECCOMP )
slirp : fix segv when init failed
qcow2 : fix possible corruption when reading multiple clusters
tpm_tis : Return TPM_VERSION_UNSPEC in case of BE failure
target - arm : Forbid mode switch to Mon from Secure EL1
target - ppc : Bug in BookE wait instruction
qdev - monitor : Avoid device_add crashing on non - device driver name
pci : Only unmap bus_master_enabled_region if was added previously
s390 : Fix memory leak
qemu - char : do not leak QemuMutex when freeing a character device
qcow2 : Correct snapshots size for overlap check
s390x / css : check ccw address validity
loader : fix a file descriptor leak
vnc : fix coverity warning
cpu : verify that block -> host is set
fix segfault in setting migration speed
nand : fix address overflow
tap : clear vhost_net backend on cleanup
target - s390x : Add missing tcg_temp_free_i64 () in disas_s390_insn (), opc == 0x8e
linux - user : In fork_end (), remove correct CPUs from CPU list
rng random backend : check for - EAGAIN errors on read
tests : fix eepro100 - test leak
s390x : reduce TARGET_PHYS_ADDR_SPACE_BITS to 62
target - ppc : fix a TCG local variable creation
coroutine : Fix use after free with qemu_coroutine_yield ()
tcg : allocate s -> op_dead_iargs dynamically
block / vvfat : Plug memory leak in enable_write_target ()
rdma : bugfix : ram_control_save_page ()
vhost / scsi : call vhost_dev_cleanup () at unrealize () time
qcow2 - bitmap : fix bitmap_free
xen - platform : Ensure xen is enabled when initializing
qemu - img : fix invalid JSON
qdev : don ' t crash on unset drive properties .
spapr : Fix undefined behaviour in spapr_tce_reset ()
hw / sd . c : Set ILLEGAL_COMMAND for ACMDs in invalid state
usb : sanity check setup_index + setup_len in post_load
cpus . c : qemu_mutex_lock_iothread fix race condition at cpu thread init
fix QemuOpts leak
cuda : fix crash on Windows
cirrus : fix off - by - one in cirrus_bitblt_rop_bkwd_transp_ * _16
suppress a couple of spurious warnings in scsi - generic . c
qdev : Fix crash on - device '?= x '
tests : fix q35 - test leaks
scsi : megasas : initialise local configuration data buffer
qom : Make object_property_set_qobject ()' s input visitor strict
tci : don ' t write zero for reloc in tci_out_label
slirp : don ' t use " smb ports = 0 " option
qcow2 : Avoid overflow in alloc_clusters_noref ()
acpi - build : disable with - no - acpi
block / drive - mirror : Reuse backing HD for sync = none
hw / scsi - bus . c : Fix use of uninitialised variable
qiov : prevent double free or use - after - free
hw / omap_gpio : Fix infinite recursion when doing 8 / 16 bit reads
qbus : fix memory leak in qbus_free ()
qemu - ga : Plug fd leak on ga_channel_listen_accept () error path
throttle : Assert that bkt -> max is valid in throttle_compute_wait ()
tests : fix check - qobject leak
rtl8139 : honor RxOverflow flag in can_receive method
vpc : Handle failure for potentially large allocations
pseries : Fix debug message for out - of - bounds address in H_PUT_TCE
pci : deassert intx when pci device unrealize
char : serial : check divider value against baud base
[ virtio - 9p ] open should not return EBADF
libqos : Correct memory leak
qapi : Avoid output visitor crashing if it encounters a NULL value
hw / pc_sysfw : Fix memory leak
qemu - sockets : Plug fd leak on unix_connect_opts () error path
qemu - img bench : Fix uninitialised writethrough mode
block / nbd : fix segmentation fault when . desc is not null - terminated
ide : Fix off - by - one error in array index check
rtl8139 : Do not consume the packet during overflow in standard mode .
target - i386 : ROR r8 / r16 imm instruction fix
slirp : fork_exec (): Don ' t close () a negative number in fork_exec ()
char : Fix socket with " type ": " vsock " address
sparc64 : fix TT_WOTHER value
acl : acl_add can ' t insert before last list element , fix
block : fix dangling bs -> explicit_options in block . c
pci : Fix crash with illegal "- net nic , model = xxx " option
virtio - 9p : Fix syntax error in debug code
exec - all . h : Use stl_p to avoid undefined behaviour patching x86 jumps
hw / isa / pc87312 : Mark the device with user_creatable = false
block : Reset buffer alignment on detach
target - arm : Stage 2 permission fault was fixed in AArch32 state
usb : fix usb - net segfault
blockdev : unset inappropriate flags when changing medium
hw / 9pfs : Fix potential memory leak and avoid reuse of freed memory
target - i386 : cpu : Fix potential buffer overrun in get_register_name_32 ()
hw / lan9118 . c : Add missing ' break ' to fix buffer overrun
Qcow2 corruption fix , by Juergen Keil .
do_snapshot_blkdev () error on missing snapshot_file argument
ide : Check validity of logical block size
target - alpha : fix decoding of CVTST / S
qemu / slirp : Fix SMB security configuration on newer samba versions
tcg / mips : Fix regression caused by typo ( copy + paste bug )
acpi - build : fix misaligned access
xen - pt : fix Out - of - bounds read
ehci : add sanity check for maxframes
exec : fix breakpoint_invalidate when pc may not be translated
target - s390x : fix CC computation for EX instruction
linux - user : assert that target_mprotect cannot fail
xen_disk : fix possible null - ptr dereference
ahci : free irqs array
pcie_aer : clear cmask for Advanced Error Interrupt Message Number
target / m68k : fix TCG variable double free
x86 : ioapic : fix fail migration when irqchip = split
tests : Fix two memory leaks
qcow2 : Prevent allocating compressed clusters at offset 0
tests : fix ipmi - bt - test leak
misc : zynq - xadc : Fix off - by - one
ppc : Let kvmppc_reset_htab () return 0 for ! CONFIG_KVM
target - ppc : Fix Temporary Variable Leak in bctar
qdev - monitor : Fix crash when device_add is called with abstract driver
qdev : Fix crash by validating the object type
fix memory leak in aio_write_f
target - arm : Signal Underflow when denormal flushed to zero on output
load_image_targphys () should enforce the max size
qapi / block : Add " fatal " to BLOCK_IMAGE_CORRUPTED
usb - storage : fix possible memory leak and missing error message
qcow2 : Put cache reference in error case
qapi : Add corrupt field to ImageInfoSpecificQCow2
virtio : zero vq -> inuse in virtio_reset ()
s390x / kvm : fix small race reboot vs . cmma
target - s390x : fix CC computation for LOAD POSITIVE instructions
slirp : tftp , copy sockaddr_size
hw / arm / allwinner - a10 : Mark the allwinner - a10 device with user_creatable = false
s390x / cpu : use g_new0
target - arm : Make arm_current_el () return sensible values for M profile
tcg : Fix compiler error ( comparison of unsigned expression )
bitops : fix test_and_change_bit ()
hw / arm / aspeed_soc : Mark devices as user_creatable = false
qcow2 : Fix header extension size check
powerpc pci : fixed packing of ranges []
hw / misc / ivshmem : Fix ivshmem_recv_msg () to also work on big endian systems
lm4549 : Fix buffer overflow
scsi : esp : check buffer length before reading scsi command
char / serial : fix copy & paste error ( fifo8_is_full vs empty )
s390x / sclp : mark sclp - cpu - hotplug as non - usercreatable
vhost : fix double free on device stop
cryptodev : add xts ( aes ) support
qga : fix ' driver ' leak in guest - get - fsinfo
tcg / optimize : optimize " op r , a , a => mov r , a "
Add a path length check to prevent heap overflow ( Eric Milliken ).
qemu - thread : fix qemu_event without futexes
virtio : error out if guest exceeds virtqueue size
intc : i8259 : Convert Array allocation to g_new0
vhost - user : check qemu_chr_fe_set_msgfds () return value
stream : fix crash in stream_start () when block_job_create () fails
virtio : decrement vq -> inuse in virtqueue_discard ()
vl : free err
scsi : Specify the xfer direction for UNMAP and ATA_PASSTHROUGH commands
ppc / xics : An ICS with offset 0 is assumed to be uninitialized
hw / arm / virt : fix cpu object reference leak
block : check for existing device IDs in external_snapshot_prepare ()
spapr : fix off - by - one error in spapr_ovec_populate_dt ()
block : Fix memory leaks in bdrv_co_do_pwritev ()
block : better error message for read only format name
iscsi : Handle failure for potentially large allocations
block : Limit size to INT_MAX in bdrv_check_byte_request ()
qdev : Use qdev_get_device_class () for - device < type >, help
scsi : Fix scsi_bus_legacy_add_drive () scsi - generic with serial
console : fix - vga none - sdl crash
i386 : define the AMD ' amd - ssbd ' CPUID feature bit
AHCI : Fix port reset race
scsi - disk : release AioContext in unaligned WRITE SAME case
qemu - char : initialize chr_write_lock
usb - storage : Drop not needed " allow_hotplug = 0 "
pci - assign : Fix potential read beyond buffer on - EBUSY
block - verify : fix 32 - bit build
i6300esb : fix timer overflow
target - arm : fix incorrect temporary variable freeing
powerpc / kvm : Fix a uninitialized bug ( Liu Yu )
ui : fix dcl unregister
tests : fix virtio - 9p - test leaks
net : make iov_to_buf take right size argument in nc_sendv_compat ()
crypto : assert that qcrypto_hash_digest_len is in range
target - arm : resource leak fixes for iwmmxt disassemble
drive mirror : fix memory leak
acpi : Assert sts array limit on AcpiCpuHotplug_add ()
ide : Remove unneeded null pointer check
ppc : fix crash in ppc system single step support
qjson : Apply nesting limit more sanely
block : Handle flush error in bdrv_pwrite_sync ()
kvm : de - register mem region for MMIO ( Glauber Costa )
vhost - user : print original request on error
pci : fix pci_info_device ().
target - i386 : Check for host features before filter_features_for_kvm ()
kvm : add error message for when SMP is requested
dump : fix memory_mapping_filter leak
hmp : fix memory leak at hmp_info_block_jobs ()
input : Add trace event for empty keyboard queue
mips_mipssim : fix use - after - free for filename
hw / vfio / platform : replace g_malloc0_n by g_new0
vhost : fix mem_sections memory corruption
lm32_uart : fix receive buffering
qcow2 : Check backing_file_offset ( CVE - 2014 - 0144 )
qom : fix leak for objects created with - object
acpi - build : fix memory leak with bridge hp off
spapr_drc : Make device " spapr - dr - connector " unavailable with - device
iothread : Make iothread_stop () idempotent
vhost - scsi : add missing virtqueue_size parameter
throttle : Set always an average value when setting a maximum value
slirp : Check qemu_socket () return value in udp_listen ()
kvm : Fix potential resource leak ( missing fclose )
linux - user : Fix invalid TARGET_ABI_BITS usage on ppc hosts
tests : fix check - qom - interface leaks
acpi : avoid potential uninitialized access to cpu_hp_io_base
vhost - user : verify that number of queues is non - zero
target / arm : Fix 32 - bit address truncation
gtk : fix possible memory leak about local_err
fdc - test : insert media before fuzzing registers
vhost : check region type before casting
exec : avoid possible overwriting of mmaped area in qemu_ram_remap
error : Copy location information in error_copy ()
linux - user / mmap . c : Avoid choosing NULL as start address
qemu - ga : Plug fd leak on ga_channel_open () error paths
chardev : use backend chr context when watch for fe
xen_pt : Present the size of 64 bit BARs correctly
qdev : Check if unplug handler exists before calling it
rdma : fix memory leak
usb : Fix usb_ep_find_packet_by_id
qemu - img : Fix memory leak
qdev : Fix qdev_try_create () semantics
block : Fix multiwrite error handling
loader : fix undefined behavior in rom_order_compare ()
gdbstub : do not restart crashed guest
armv7m : R14 should reset to 0xffffffff
vl : fix memory leak spotted by valgrind
linux - user / syscall . c : remove warning : ‘ array ’ may be used uninitialized in this function
qemu : msix nit : clear msix_entries_nr on error
crypto : fix mistaken setting of Error in success code path
qlist : Make conversion from QObject * accept null
qemu - img : Fix leakage of options on error
hw / pci / pci_host . c : Avoid shifting left into sign bit
block : Remove redundant assertion
apic : kvm - apic : Fix crash due to access to freed memory region
target - i386 / kvm . c : Code formatting changes
qcow2 : Free allocated snapshot table on error
vl : Add failure check for SetEvent
Check the returned audio_buf_info fields
spapr : fix possible memory leak
usb : ehci : fix memory leak in ehci_process_itd
xenfb : use the correct condition to avoid excessive looping
block : Fix serial number assignment
dcbz fix ( Jocelyn Mayer )
Initialize a variable in all cases
spapr / htab : fix savevm
hw / display / tcx . c : Fix memory leak
spapr : support only ELF kernel images
pc - dimm : don ' t assert if pc - dimm alignment != hotpluggable mem range size
block / vvfat : Plug memory leak in read_directory ()
ui / console : fix OVERFLOW_BEFORE_WIDEN
console : Fix warning from clang ( and potential crash )
vhost : fix dirty page handling
qemu - io : fix a memory leak
sockets : Plug memory leak in socket_address_flatten ()
tcg : correctly mark dead inputs for mov with a constant
qemu - timer . c : Trim list of included headers
target - i386 : Fix apic object leak when CPU is deleted
xen_disk : mark ioreq as mapped before unmapping in error case
ppc64 : fix compressed dump with pseries kernel
vnc : fix incorrect checking condition when updating client
cpu_physical_memory_write_rom () needs to do TB invalidates
qga : fix uninitialized value warning for win32
tap : clear vhost_net backend on cleanup
aio - posix : honor is_external in AioContext polling
cpu - exec : Also reload CPUClass * cc after longjmp return in cpu_exec ()
slirp : fix structure initialization in tcp_listen ()
mptsas : fix memory leak
qemu - option : Make qemu_opts_foreach () accumulate return values
linux - user : getpriority errno fix
block / nbd : Fix the leaked visitor
gtk : fix uninitialized temporary VirtualConsole
win32 - aio : Fix memory leak
s390x / css : start with cleared cstat / dstat
spapr_vscsi : Set uninitialized variable
pci : fix double free of romfile property
scsi : Add assertion for use - after - free errors
parallels : fix default options parsing
qemu - img : Plug memory leak in convert command
dirty - bitmap : Report BlockDirtyInfo . count in bytes , as documented
audio : Don ' t free hw resources until after hw backend is stopped
blockdev : Plug memory leak in drive_uninit ()
sheepdog : fix resource leak with sd_snapshot_create
hw / arm : Mark the " fsl , imx25 " device with user_creatable = false
io : remove mistaken call to object_ref on QTask
vhost - net : do not crash if backend is not present
xen / pt : Set is_express to avoid out - of - bounds write
hw / mips_malta : remove redundant irq and clock init
fixed memory leak
block : Catch ! bs -> drv in bdrv_check ()
disas / i386 : Avoid NULL pointer dereference in error case
qemu - ga : Plug file descriptor leak on ga_open_pidfile () error path
KVM : Use return value for error print
qga : fix off - by - one length check
vl : Tighten parsing of - numa ' s parameter mem
console : add same surface replace pre - condition
pci : memory leak of PCIDevice :: rom_file
exec : always use MADV_DONTFORK
ide : only constrain read / write requests to drive size , not other types
tests : fix leak in test - string - input - visitor
hw / virtio - 9p - local . c : Remove unnecessary null char in symlink file
spice : abort on invalid streaming cmdline params
ide : refuse WIN_READ_NATIVE_MAX on empty device
ui / curses . c : Ensure we don ' t read off the end of curses2qemu array
Add a pc - 0 . 11 machine type and make the pc type an alias
target - mips : save cpu state before calling MSA load and store helpers
target - alpha : Fix compiler warning for gcc - 4 . 3 ( and older )
usb - smartcard - reader : Properly NAK interrupt eps when we ' ve no events
valgrind : avoid false positives in KVM_GET_DIRTY_LOG ioctl
linux - user : Fix error handling in flatload . c target_pread ()
tests : check - qom - proplist : fix leak
hw / openrisc : Avoid using uninitialised variable ' entry '
qemu - sockets : use qapi_free_SocketAddress in cleanup
ui : sdl2 : Release grab before opening console window
dma : rc4030 : limit interval timer reload value
ehci : fix overflow in frame timer code
DOS 6 . 22 fix ( Johannes Schindelin )
pci : Use of qemu_put_ram_ptr in pci_add_option_rom .
rbd : Fix leaks in rbd_start_aio () error path
sd : Avoid access to NULL BlockDriverState
qdev - monitor : fix segmentation fault on qdev_device_help ()
qapi : Fix c_name () munging
thread - pool : avoid deadlock in nested aio_poll () calls
linux - user / syscall . c : lock_iovec : unlock vec [ i ] in failure processing code block
json - parser : add handling for NULL token list
linux - user : fix file descriptor leaks
input : Decrement queue count on kbd delay
hmp : fix crash in ' info block - n - v '
vmdk : Fix overflow if l1_size is 0x20000000
target - i386 : Fix mulx for identical target regs
linux - user : zero fstat buffer to initialize nsec fields
slirp : fix CVE 2007 - 5729
qemu - option : Fix uninitialized value in append_option_parameter
qxl : clear irq on reset
ccid - card - emul : do not crash if backend is not provided
linux - user : remove ifdef __USER_MISC
qemu / iovec : Don ' t assert if sbytes is zero
linux - user : Correct type for BLKSSZGET
virtio - net : fix guest - triggerable buffer overrun
vfio / ccw : allocate irq info with the right size
fix bug in block - qcow2 . c : alloc_cluster_offset () ( Shahar Frank )
qcow2 : Fix L1 allocation size in qcow2_snapshot_load_tmp () ( CVE - 2014 - 0145 )
rtl8139 : check IP Total Length field ( CVE - 2015 - 5165 )
cadence_uart : Fix buffer overflow
usb - redir : Fix crash on migration with no client connected
qxl : Fix new function name for spice - server library
qcow2 : Remove dead check on ! ret
qemu - common : add QEMU_ALIGN_DOWN () and QEMU_ALIGN_UP () macros
qcow2 : fix offset overflow in qcow2_alloc_clusters_at ()
exit if - drive specified is invalid instead of ignoring the " wrong " - drive
usb : fix up post load checks
target - ppc : fix nego and subf * o instructions
hw / arm / virt : Fix corruption due to double free
block : Fix bdrv_all_delete_snapshot () error handling
x86 / cpuid : Plug memory leak in cpudef_setfield ()
s390 : fix reset hypercall to reset the status
cpu : don ' t allow negative core id
linux - aio : Fix laio resource leak
ppc : fix double - free in cpu_post_load ()
qapi / string - output - visitor : fix human output
posix - aio - compat : Plug memory leak on paio_init () error path
qemu / migration : fix the double free problem on from_src_file
hw / arm / virt : Check that the CPU realize method succeeded
qemu - timer : fix off - by - one
ui : fix crash with sendkey and raw key numbers
ivshmem : fix memory_region_del_eventfd assertion failure
s390x : silence warning from GCC on uninitialized values
char : fix missing return in error path for chardev TLS init
qemu - char : qemu_chr_open_fd () can ' t fail , don ' t check
mirror : Fix resource leak when bdrv_getlength fails
target - s390x : Mark check_privileged () as ! CONFIG_USER_ONLY
hw / core / loader : Fix possible crash in rom_copy ()
vnc : fix segfault in vnc_display_pw_expire ()
acpi : add dummy write function for acpi timer
virtio : increase the queue limit to 1024
watchdog / wdt_diag288 : Mark diag288 watchdog as non - hotpluggable
audio : fix WAVState leak
ossaudio : fix memory leak
pci - devfn : check that device / slot number is within range
spapr_drc : Fix potential undefined behaviour
flush TLBs at cpu reset
container : don ' t leak container reference
chardev / baum : fix baum that releases brlapi twice
fix return check for KVM_GET_DIRTY_LOG ioctl
slirp : fix use - after - free
vvfat : Fix compilation with DEBUG defined
page_cache : fix memory leak
block / vvfat : Plug memory leak in check_directory_consistency ()
runstate / migrate : Two more transitions
mempath prefault : fix off - by - one error
qcow2 : Fix sector calculation in qcow2_measure ()
arm_gicv3 : Fix broken logic in ELRSR calculation
xio3130_downstream : Report error if pcie_chassis_add_slot () failed
virtio - net : fix buffer overflow on invalid state load
usb : free leaking path
hw / usb / bus : Remove bad object_unparent () from usb_try_create_simple ()
qemu - socket : set passed fd non - blocking in socket_connect ()
isapc : fix segfault .
hw / intc / arm_gicv3_common : Give no - migration - shift - bug subsection a needed function
target - mips : Fix type cast for w64 ( uintptr_t )
pci : fix corrupted pci conf index register by unaligned write
block / gluster . c : Handle qdict_array_entries () failure
input : don ' t queue delay if paused
virtio : correctly initialize vm_running
vga : check the validation of memory addr when draw text
qcow2 : don ' t leak buffer for unexpected qcow_version in header
qcow2_check : Mark image consistent
target - i386 : Don ' t strdup () alias property name
qom : Fix memory leak in function container_get
scsi : fix warning
block : Assert validity of BdrvActionOps
vnc : Fix fatal crash with vnc reverse mode
hw / arm : Mark the " fsl , imx6 " device with user_creatable = false
target - i386 : fix Coverity complaints about overflows
sheepdog : don ' t leak socket file descriptor upon connection failure
block / raw - posix : Fix disk corruption in try_fiemap
target - arm : Correct " preferred return address " for cpreg access exceptions
hw / ppc / spapr_rtc : Mark the RTC device with user_creatable = false
raw_bsd : don ' t check size alignment when only offset is set
hw / 9pfs : Fix crash when mounting with synthfs
hw / pci : fixed error flow in pci_qdev_init
spapr : fix memory leak in spapr_memory_pre_plug ()
aio - win32 : fix uninitialized use of have_select_revents
mirror : Handle failure for potentially large allocations
qcow2 : Check min_size in qcow2_grow_l1_table ()
ppc : Fix bug in handling of PAPR hypercall exits
sdl2 : fix crash in handle_windowevent () when restoring the screen size
usb - redir : fix use - after - free
qdev : Change Property :: offset field to ptrdiff_t type
qdev : fix thinko leading to guest crashes
target - i386 : Use 1UL for bit shift
i386 ROR r8 / r16 instruction fix
vmdk : Fix big flat extent IO
pty : Fix byte loss bug when connecting to pty
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
block / vpc : make checks on max table size a bit more lax
slirp : Fix memory leak on small incoming ipv4 packet
tcg : Reload local variables after return from longjmp
vfio - pci : Further fix BAR size overflow
qcow2 : Fix potential qemu - img check crash on 32 bit hosts
vt82c686 : avoid out - of - bounds read
qcow2 corruption : Fix alloc_cluster_link_l2 ( Kevin Wolf )
vmdk : Widen before shifting 32 bit header field
scsi - bus : Fix transfer length for VERIFY with BYTCHK = 11b
qemu - sockets : Fix potential memory leak
hw / ppc / spapr_cpu_core : Add a proper check for spapr machine
rtl8139 : check TCP Data Offset field ( CVE - 2015 - 5165 )
xen / pt : xen_host_pci_config_read returns - errno , not - 1 on failure
qemu - sockets : Fix buffer overflow in inet_parse ()
tests : fix coroutine leak in / basic / entered
virtio : add missing mb () on enable notification
usb - host : Initialize dev -> port the obviously safe way
block / iscsi : fix memory corruption on iscsi resize
hw / mips_malta : Fix YAMON API print routine
sheepdog : Fix crash in co_read_response ()
linux - user : avoid gcc array overrun warning for sparc
sheepdog : Fix misleading error messages in sd_snapshot_create ()
hw / scsi / spapr_vscsi : Remove superfluous memset
qemu - img : fix failed autotests
block : Catch integer overflow in bdrv_rw_co ()
fix build failure in nbd_read_reply_entry ()
fix double free the memslot in kvm_set_phys_mem
qxl : stop dirty loging when not in vga mode
vfio / pci : Fix regression in MSI routing configuration
vnc : avoid possible file handler leak
pseries : Fix array overrun bug in PCI code
qxl : simplify qxl_rom_size
input : fix memory leak
s390x : fix cpu object referrence leak in s390x_new_cpu ()
pc_sysfw : Fix ISA BIOS init for ridiculously big flash
target - i386 : avoid overflow in the tsc - frequency property
qcow2 : remove memory leak
sockets : avoid crash when cleaning up sockets for an invalid FD
blockjobs : fix set - speed kick
rtl8139 : skip offload on short Ethernet / IP header ( CVE - 2015 - 5165 )
spapr_cpu_core : drop reference on ICP object during CPU realization
mirror : make sectors_in_flight int64_t
xlnx - ep108 : Fix minimum RAM check
spapr : fix leak in h_client_architecture_support ()
e1000 : flush packets when link comes up
spapr : fix write - past - end - of - array error in cpu core device init code
tap - win32 : skip unexpected nodes during registry enumeration
qemu - char : don ' t leak opts on error
QOM : Fail casts for unknown types
ccid : Fix buffer overrun in handling of VSC_ATR message
fsdev : fix virtfs - proxy - helper cwd
qemu - img : Fix crash in amend invocation
linux - user / signal . c : Don ' t pass sigaction uninitialised sa_flags
target - arm : check that LSB <= MSB in BFI instruction
block / qcow . c : Fix memory leak in qcow_create ()
cpus . c : add additional error_report when ! TARGET_SUPPORT_MTTCG
virtio : add check for descriptor ' s mapped address
block : vhdx - add error check
target - arm : Fix TCG temporary leaks for scalar VMULL
qxl : clear guest_cursor on QXL_CURSOR_HIDE
target - arm : Reinsert missing return statement in ARM mode SRS decode
target - i386 : Don ' t left shift negative constant
tests / bios - tables - test : check the value returned by fopen ()
irq : fix memory leak
scsi - generic : check the return value of bdrv_aio_ioctl in execute_command
acpi : fix assert failure caused by commit 35c5a52d
qemu - ga : become_daemon (): reopen standard fds to / dev / null
target - ppc : fix left shift overflow in hpte_page_shift
hw / display / framebuffer . c : Avoid overflow for framebuffers > 4GB
qcow2 : Assert against snapshot name / ID overflow
io : avoid double - free when closing QIOChannelBuffer
PPC : KVM : Synchronize regs on CPU dump
spapr_vty : lookup should only return valid VTY objects
vl : Set errp to & error_abort on machine compat_props
i386 : hvf : inject General Protection Fault when vmexit through vmcall
io : assert errors before asserting content in I / O test
qcow2 : Fix alloc_clusters_noref () overflow detection
spapr : drop reference on child object during core realization
cpu : Don ' t clear cpu -> exit_request on reset
vl : exit qemu on guest panic if - no - shutdown is not set
sd : sdhci : check data length during dma_memory_read
spapr : ensure we have at least one XICS server
exec : Add missing rcu_read_unlock
APIC : Do not start zero - period timers ( Jan Kiszka )
linux - user : don ' t crash with null name
vga : Silence bogus gcc warning about uninitialized variables
vmdk : fix buf leak in vmdk_parse_extents ()
hpet : Fix emulation of HPET_TN_SETVAL ( Jan Kiszka )
chardev : clear O_NONBLOCK on SCM_RIGHTS file descriptors
slirp : don ' t crash when tcp_sockclosed () is called with a NULL tp
ivshmem : Fix fd leak on error
hw / arm / omap_gpmc : Avoid buffer overrun filling prefetch FIFO
tcg - sparc : Fix small 32 - bit movi
target - microblaze : dec_barrel : Plug TCG temp leak
qemu - img convert : Use cache = unsafe for output image
pci : move dereferencing of root only after verifying valid root pointer
fsdev / virtfs - proxy - helper : Fix improper use of negative value
fsdev : Fix potential memory leak
vvfat : fix out of bounds array_get usage
stdio_pclose : return - errno on error ( v3 )
target - tricore : Fix helper_msub64_q_ssov not reseting OVF bit
blockdev : reset werror / rerror on drive_del
vpc : fix return value check for blk_pwrite
arm : fix compile on bigendian host
qcow2 : Use sizeof (** refcount_table )
hw / arm_gic . c : Define . class_size in arm_gic_info TypeInfo
prevent a possible double - free
fix memory leak
add warning to zos command (# 8585 )
versioninfo : Fix SEGV on files with no info (. txt , ...)
take bits " wildcard " into account in r_bp_size (# 8994 )
fix oob in dex
Fix a gcc warning
CID 1134732 Out - of - bounds access / libr / asm / arch / avr / format . c
fix hang in version info ELF
bin : Delete the list of objs instead of deleting current object
riscv : Return actual instruction length
Initialize all bits in the sigaction .
varnishncsa : Don ' t output ESI subrequests
Delete the right list item .
note a TODO
Bail out of CLI reads on signals
extra check in mg_start (): options may be NULL
Added connection - specific conn_data pointer to struct mg_request_info
missing list close
plugins / emperor_amqp : fix leak on error path
attempt to fix # 713
core / alarm : fix memory leak in uwsgi_alarm_thread_loop
added a security check in the http router
check for mules before nuke
realpath on BSD is different from Linux / Solaris / OSX
fixed a bug in setrlimit
plugins / python : check uwsgi_connect return code in uwsgi_python_harakiri
fixed a bug with -- set
plugins / v8 : fix double free
threads must be implicit in PyPy
report socket name on invalid zeromq syntax
plugins / router_cache : fix memory leak in uwsgi_router_cache_store
fixed sigquit / sigint in hijacked mode
fixed goodbye_cruel_world in gevent mode
Added an additional check in some code to avoid dividing by zero ...
Attempt to appease travis
Units / Map : added an empty () function
Try to fix bug # 8885 ( negative gold amount gets changed while save / load )
updated about : Guillaume Duwelz - Rebert removed from developers
Set the core_id preferences item at game launch .
Correcting a comment .
Added dedicated fallback message if a replay is loaded from mp create
Fixed huge cpu usage when the window is hidden .
fixed bug where the program could freeze up when networked
Initialize all members .
dont allow empty indexes in wml variables
fix default room creation policy on server
add game start notification
Fix : with -- new - widgets advanced preferences may not shrink on deselect
Add a getter for need_layout_ .
AI : ensure multiple weapons are always checked for [ disable ] special
Initialize all members .
fixup ! Add DEPRECATED macro and remove uses of snprintf
Fixed type of temporary variable in _TIFFCheckMalloc () as per bug
fix possible OOB write in gif2tiff . c
* tools / tiffcrop . c : fix readContigStripsIntoBuffer () in - i ( ignore ) mode so
fix unchecked malloc result
* tools / tiff2pdf . c : avoid potential heap - based overflow in
cmd : Use ' void ' instead of empty parameter list
lxc_abstract_unix_connect : accomodate containers started before Oct 28
terminal : fix uninitialized variable in strerror () log message
sd - network : fix memleak in dhcp6_option_parse_domainname (# 5114 )
[ PATCH ] fix spurious valgrind warning in udev .
sysusers : fix memleak (# 4443 )
sd - dhcp - server : fix a leak
systemctl : plug a leak upon create_symlink mismatch
install : explicitly return 0 on success
systemctl : fix memory leak (# 7289 )
sd - login . c : fixup for d70964d0
seccomp : add proper help string for @ resources seccomp filter set
ude_rules . c : fix possible NULL pointer dereference in get_key ()
rfkill : Avoid error when state restore is disabled
terminal : discard async read () errors for evdev
core : kill processes started due to the ExecReload = on timeout
tmpfiles : add missing OOM check
core / dbus - execute : actually set PassEnvironment = (# 7510 )
terminal / screen : perform bold -> light conversion only on foreground
systemctl : fix check for template units
basic / strv : use existing qsort_safe () helper
sd - event : fix theoretical leak on error in event_make_signal_data
shared : fix search_and_fopen with alternate roots
logind : fix printf format
systemctl : do not print header if no units will be listed
fuzz : fix coding style
systemctl : Add missing "/" to files created by ' edit -- runtime '
basic : use for () loop instead of while ()
sd - dhcp - client : fix busy loop reading zero sized DHCP UDP packets . (# 3299 )
path - util : plug leak
bus - util : fix error handling
sd - bus : fix object tree to be deeper than 2 levels
fstab - util : detect out - of - range pri = assignments
mount : fix for complex automounts
util : make sure we don ' t clobber errno in error path
udev : settle should return immediately when timeout is 0
cgroup : the root cgroup is always populated
bus : in sd_bus_try_close () consider local read queue too
locale : make sure that l is freed
terminal / subterm : use usec_t instead of " unsigned long "
base - filesystem : avoid all searching if the link already exists
machinectl : make sure we are not reading an unitialized variable
switch - root : not only mkdir the parents but the target dir
util : be a bit safer in path_is_safe ()
journalctl : don ' t choke on entries with no MESSAGE = field
json : avoid cleanup of unitialized variable
journald : fixed assertion failure when system journal rotation fails (# 9893 )
random - util : we are fine if ints are 16 bytes actually
core / manager : Make sure jobs statistics are not double after daemon - reload
bus_open leak sd_event_source when udevadm trigger 。
resolved : bugfix of null pointer p -> question dereferencing (# 6020 )
calendarspec : free memory when parsing fails (# 4890 )
sd - dhcp - client : readd deleted error check
libudev : make valgrind happy
sd - resolve : fix check for packet size
exit - on - idle : only exit if actually idle
usb_id : MassStorage SubClass 6 is " scsi " not " disk "
readd early log level check for performance reasons .
Patch to log security warnings when invalid identifier is used in message
saEvtEventAttributesSet needs to check that the priority value passed in
set serialize lock to proper function .
Stop the totem stats updater timer during shutdown to prevent references to
cleanup valgrind warnings .
totemsrp : Enhance mcast failure detection
libdocument : return 96 DPI if unable to determine true value
[ windows ] init locale_dir only once
libview : Add check for pageless documents to ev_view_accessible_focus_changed
[ libview ] Do not jump to link dest when clicking with GDK_CONTROL_MASK
libview : Do not schedule a redraw when selecting all text
Bug 493778 – metadata plug - in crashes on some images
app : don ' t leak the GtkTextBuffers of Gegl multiline text properties
libgimpconfig : support $( gimp_installation_dir ) in config paths
app : make LCH lightness select - criterion faster
app : clear current path from the context after copying it
plug memory leak : Fixes # 305990
app : prevent warnings in gimp_levels_tool_config_notify ()
Bug 791514 - Cannot export to webp file .
app : update ` gimp - xcf - save ` procedure documentation .
Make the preview column the expander column
libgimp : disable tile command range check in plug - in tile backend
Bug 595605 - memory leaks in plug - ins / common / curve - bend . c
plug - ins : Use string literal as format in metadata plug - in
app : initialize variable in gimp_histogram_view_update_bins ()
app : finish porting GdkScreen / monitor_number to GdkMonitor
app : set an icon on the floating selection filter
Bug 686379 : In the JPEG save dialog , checking / unchecking XMP ...
app : add a tooltip to the color temperature preset menu button
Bug 701120 : Opening fuzzed . pnm - file causes stack buffer overflow
Bug 645120 - Disable color tools overlay dialogs
app : avoid id . matrix short - circuit in trans . tool when ...
[ ot - font ] Fix hmtx wrong table length check
Bug 528272 – " Error loading GPOS table 5503 " when using katakana
[ Indic ] Disable ' kern ' in Uniscribe bug compatibility mode
make it compile with - ansi - pedantic
Remove a " register " keyword from C ++ code
Tabname defaults to the table name for MONGO tables
branches / zip : btr_search_guess_on_hash (): Do not call buf_block_get_frame (),
Remove the temporary file created by mysql_upgrade
Fixed a typo , which prevented non - valgrind build .
Whoops , freed something not malloced
branches / zip : Restore the transaction commit that was removed in r1583 .
branches / zip : btr_cur_optimistic_insert (): Remove a redundant condition .
initialize mysql -> charset in mysql_init
branches / zip : buf_page_init_low (): Initialize also flush_type .
fixed bug # 1661 Compiling -- pstack fails with binutils 2 . 13 . 90
sync0arr . c : Fix compilation error on some systems by introducing all
IB : production compilation fix
branches / zip : row_merge_create_temporary_table (): On error , free the
do not ignore first char , when truncating spaces in str -> num conversion .
branches / zip : row_sel_sec_rec_is_for_clust_rec (): Augment the bug fix in r918 :
branches / zip : Deny non - superuser access to the INFORMATION_SCHEMA
branches / zip : ha_innobase :: add_index (): Check for ! innodb_table .
followup to SET PASSWORD fix
Added missing prototypes to mysys / my_bitmap . h
refs # 5560 , fix bug
allocate an array of pointers to ctpair ' s rather than an
Addresses # 1683 Assert shutdown is not called twice on the same minicron
branches / 5 . 1 : Use btr_free_root () instead of fseg_free () for
Fix a warning about extra parenthesis
branches / zip : buf_LRU_block_remove_hashed_page (): Add some debug assertions .
trx_undo_parse_page_header (): Silence a bogus warning .
[ t : 4472 ], add table lock for truncate
connect zip bug fix
Applying InnoDB snashot 5 . 0 - ss5406 , part 1 . Fixes BUG # 38479
Merge a patch from MySQL AB ( Mats Kindal ):
[ t : 5131 ] closes # 5131 .
Applying InnoDB snapshot
fixed picture in comment .
fixed BINLOG crashes in case of badly formed statement ( Bug # 17457 )
fix for decimal . c
bugfix : delayed insert table was using other table ' s expr_arena
[ trunk ] Fix Heap - based buffer - overflow when decoding openjpeg image
r15723 @ tombo : nickm | 2007 - 12 - 26 19 : 25 : 51 - 0500
Fix an off - by - one buffer size in dirserv . c that magically never hit our
Add a missing return - 1
Fix a double - free in rend_config_services ()
Fix a NULL pointer dereference in parse_server_transport_line ().
fix crash introduced in r9622
Generate our x509 certificates using sha256 , not sha1 .
Fix a check - spaces complaint
r11753 @ Kushana : nickm | 2006 - 12 - 29 11 : 30 : 28 - 0500
fix memory leak introduced in 26e89742
fix a memory leak
clean up a log message
fix a seg fault with exitredirect
Treat a bad tor_spawn_background () as a BUG ().
r7341 @ Kushana : nickm | 2006 - 08 - 11 00 : 51 : 05 - 0700
Harmless : Z_OK is not an acceptable answer to Z_FINISH .
Fix a potentially useless integer overflow check .
initialize memory to remove Valgrind warning ( Zooko , Jeffrey Walton )
fix ECP curve BER decode
Added < utility > based on C ++ 11 for R - values and std :: move
Reverted change in config . h as it does not fix anything .
fix erasepage on linux
fix jsvGetString regression
fix precedence of ' void 0 '
wow - 2016 now
fix wifi restore ssidAP
replace signed with unsigned counter to avoid warning when compiling with
svn merge - r16071 : 16073 https :// eris . llnl . gov / svn / slurm / branches / slurm - 1 . 3
Initialize the entire launch structure .
change the pthread locking to eliminate a race condition
fix for checking valid qos
fixed last patch
Prevent slurmctld from dumping core ifjob_resrcs is missing in the
Initialize variable to eliminate clang error
check for NULL pointer
fix for qos char list
fixed minor memory leak
Prevent zero divide
Missing batch job given exit code of 1
Always pass non - NULL status reference to job_waitjid , that function in
Decrease the time of batch start time no more than DEFAULT_MSG_TIMEOUT .
fix extra space
set version to 0 . 5 - dev
fixed small leak in msRemoveStyle
Fixed bug 769 , duplicate error with TILEITEM .
Added the declaration for freeStyle to eliminate the unresolved externals .
fix leak on group_layers
don ' t free any static memory
Changed default styleObj offsets to 0 , duh ...
Set the WSQ TLS key to inited after initing it .
[ runtime ] Speed - up MERP timeout
do not exit after the first read !
[ sgen / mono ] Don ' t leak stack info struct
Fix the previous change .
[ llvm ] Fix 32 bit platform support which got broken after c678533842f10356968c203ed8fa43e86a406c6e .
tasklets . c : update used stack size
Fix a bug introduced by the previous change to mono_jit_walk_stack_from_ctx_in_thread ().
[ coop handles ] Fix monotonic handle check race with stw in mono_handle_new
[ runtime ] Free leaked variable . (# 4643 )
[ sgen ] Save more space in M & S block infos .
[ runtime ] Don ' t free null cache hash table in aot compiler
[ llvmonly ] Avoid disabling llvm for methods with pinvoke signatures in llvmonly mode .
[ profiler ] Replace a check with an assert in coverage_filter ().
[ sgen ] We must do bridge processing with all sgen locks taken .
Detach the current thread as soon as possible when mono_gc_pthread_exit is called .
[ sgen ] Fix names of descriptor types in describe_ptr ().
[ runtime ] Free LLVM failure message
[ runtime ] Fix malloc backend for mempools .
Ignore seq points of entry and exit IL offsets .
Fix the build .
[ runtime ] Make mono_trace_set_printerr_handler () call g_set_printerr_handler (). Fixes # 31451 .
[ runtime ] Free temporary attr structure
[ jit ] Avoid emitting seq points in inlined methods .
[ jit ] Fix the handling of byref locals in mono_allocate_stack_slots2 ().
Fix a JIT crash when - v - v - v - v is used .
[ aot ] Fix a memory leak .
make sure destructors are called , cleaning up socket
dnsdist : Fix a sign - comparison warning in isEDNSOptionInOpt ()
dnsdist : Flush output in single command client mode
auth : Reset the TSIG state between queries
add a dropAction () creator for Lua
Initialize d_result to NULL on ctor
Added code to randomize values returned by rand ()
Prevents a possible memory leak
Added missing free ()
Fixed possible NULL pointer exception in ssl_get_ciphersuite ()
list - view : don ' t crash when expanding a directory
desktop - background : stop any pending crossfades on widget destruction
have to call delete_files before done ; oops
pathbar : set right return value for class method function pointer
link : plug a memory leak
window - slot : fix formatting
Ignore > 2 clicks in the icon container (# 621098 ).
mime - actions : fix double free in error path
files - view : select the pending selection
search - hit : fix mem leak
eel - canvas : ignore extra mouse button events
window : open detached tab windows at mouse position
fixed # 1783 ( fuzz )
fix buffer overrun in gf_bin128_parse
[ bouqueau ] fix win32 / mobile build
Fixed crash introduced by trafID checks patch
fix XML dump when file does not have tracks ( e . g . HEIF files )
Commented safety check by romain breaking SVC import
Removed scanning files starting with '.'
fix for v1 infe ( MPEG conformance file )
fixed potential crash in adts reframer with broken streams - cf # 1723
fixed UNKNOWN READ
jcop : coverity scan warning RETURN_LOCAL
fix trivial leak
Prevent leaking memory from PIV readers
use path only if it is actually present
OpenPGP : NULL - ify free ()' d pointer
Made sure attribute CKA_TOKEN is set to TRUE .
sm - common : fixed resource leak
reader - pcsc . c : fixed pin checking when implicitly given
coolkey : Make sure the object ID is unique when filling list
libopensc : initialize value returned by sc_select_file
pkcs15 : log parsed TokenInfo . serial
fixed parsing pace output data
libopensc : initialize ' sc_ef_atr ' type variable
prevent out of bounds write
card - westcos . c : fixed potential resource leak
added a check for CKA_CERTIFICATE_TYPE to attr_extract ()
psql : honor ' footer ' option for expanded output
Check that result tupdesc has exactly 1 column in return_next scalar case .
Add an explicit cast to Size to hyperloglog . c
[ Fix ] Simplexmlelement should throw an exception rather than fatal
Fix a bug in handleAssertionEffects
exif_scan_JPEG_header : Added length check to prevent information leak
ext_mbstring : Fix invalid free () in php_mb_parse_encoding
adding Content - Length for HEAD request
Fix a fallthrough bug in TranslatorX64 :: smash
Fix a copy - paste error
tdf # 96243 don ' t crash if LibO install . wasn ' t found
[ XrdCl ] Prevent segv in case user has no name .
XrdHttp : Add missing return statement
Recognize the -- coerce xrdcp option ( it was forgotten ).
[ XrdCl ] Treat empty redirect response as error .
[ SSI ] Unbind the request prior to teardown .
chanserv / sync : Return after kicking for RESTRICTED
anope_convert : Avoid a crash on a nickless account .
libathemecore / help : Fix memory leak when a help file cannot be opened
chanserv / moderate : don ' t moderate registrations from those who have PRIV_CHAN_ADMIN .
fixed sendfile core dump .
added more extension info .
Missing delete for config reader
Copy the silence flags when sending update notifications .
Fixed some craq ++
Fixed / spynames , thanks to relax for finding the bug
Made erroneous included configs cause the reader to bail
browser : check window liveness before setting title
win : Fix setting icon after format of atom . rc has changed .
added comment to mention sk_tools_utils :: copy_to
Closing asar file should be syncronous
unzip : comment why there is no USE_INTERNAL_MINIZIP boolean option
* Fix to the gcc4 /- O0 x86 JIT compiler bug
Invalid values for saved_stamp in comput_subject_downtime_times ()
zebra : stack overrun in IPv6 RA receive code ( CVE - 2016 - 1245 )
pimd : Fix size_t zlog_err format string warning
ospfd : fix packet reception for FreeBSD 10 .
ospf6d : fix off - by - one on display of spf reasons
bgpd : prevent double address delete on shutdown
* src / transform . c ( transform_save ): append newline to old text
* src / augeas . c ( init_root ): check for failure of strdup
* examples / fadot . c : free the result from FA_AS_REGEXP
avoid symlink attacks in __fish_print_packages and spawning fishd
env_universal_common : always pick shmem strategy on Cygwin
fish_indent : indent " case " with the same level as " swith "
allow std :: string in setContentType as before
reset stop flag before starting threads
prevent out - of - bounds ensure clause access ; fix # 2910
undef append_features for Classes
fix String # inspect with MRB_UTF8_STRING
pluto : dpdaction = restart folloup to 24f14ba85ed250b
make sure to unpack the nonce that was calculated .
set the received message to zero , to make sure
pluto : undo bug introduced in 0395c26ac17af9b02e4f1b84d7279288fc985ced
LSW_FDMASK : Avoid signed integer overflow
eliminate an LLVM warning about an unused value
x509 : free shallow general names after requesting crl fetch
proper fix for mtu calculation initialization
fix typo . the previous commit brome make .
" Memory leak . Dynamic memory stored in ' file_content . buf ' allocated
git - svn - id : svn :// cherokee - project . com / cherokee / trunk @ 474 5dc97367 - 97f1 - 0310 - 9951 - d761b3857238
Fixes a tiny memory leak on the Digest ' nonce ' cache code .
Fixes a memory management issue . The ' extensions ' rule match could
compose : fix infinite loop in parser on some inputs
src / utils : check if fileno () failed in map_file
xkbcomp : Don ' t explode on invalid virtual modifiers
xkbcomp : fix pointer value for FreeStmt
xkbcomp : Don ' t falsely promise from ExprResolveLhs
Bionic : Aliasing problems with frexpf
Remove a gcc - 4 . 5 compilation warning :
fix PTRDIFF_MAX check in the 32 - bit mmap wrapper
bionic : fix memory leak in get_malloc_leak_info () error path
Libraries without dt_soname are inaccessible
fix crash in tracing objtypespec
Free the WSTR version of the filename when dead .
swb : fix one missing inlineSlots annotation
Fixed a memory leak in FileRef ' s OGA format detection
Added raptor_new_identifier_from_double pointer
Fixed new bug due to a typo in the PNG00 fix
squash heap - buffer - overflow , PoC TIFF from Hardik
Added check for incorrect number of meta channels report in # 492 .
Added missing null check reported in # 817 .
Fixed SEGV reported in https :// github . com / ImageMagick / ImageMagick / issues / 130
Added missing return to avoid use after free .
Forgot to factor in softness
Restored call to RelinquishMagickResource .
Added missing call to ConstrainColormapIndex .
Fixed incorrect call to WriteBlob reported in # 490 .
Fixed incorrect call to DestroyImage reported in # 491 .
Added check for bit depth 1 .
Fixed head buffer overflow reported in : https :// github . com / ImageMagick / ImageMagick / issues / 98
Add another trace point .
fixed a warning in client ( mac )
fixed a server crash
fix a leak when unpausing evrpc requests
reset " chunked " flag when sending non - chunked reply
avoid valgrind false positive by zeroing epoll_event
Fix a reversed check in upcast_evbuffer
r15245 @ tombo : nickm | 2008 - 04 - 18 09 : 27 : 50 - 0400
initalize ev_res from Scott Lamb
* ftpclass . cc : don ' t send FEAT after QUIT .
* FileCopy . cc : don ' t create target file if source does not exist .
correct a signedness warning
Fixed memory leak
win : add missing break statement
Windows : set loop counters to zero in uv_loop_init
unix : don ' t run i / o callbacks after prepare callbacks
unix : fix off - by - one error in comparison
sunos : filter out non - IPv4 / IPv6 interfaces
unix : handle EWOULDBLOCK in uv__write ()
X - E3 to camera list
Plugin : add export to buffer_size function .
palm : getpeername may fail , found by coverity
mk_core : thread : fix ucontext . h path
Duda : SQLite : fix return value on sql_dump ()
utils : Fix a valgrind warning about uninitialized data
Sched : Fix a segfault when iterating timeouts
mk_server : streams : fix dereference after null check ( CID 1312109 )
shell - doc - system : fix %- escaping code in shell_doc_system_open ()
recorder : Don ' t compute the buffer size twice
st - box - layout : Don ' t report a paint volume when we have no allocation
browser - plugin : Set that we need XEmbed
[ ShellApp ] Signal windows - changed when removing a window
shell - global : Use G_VARIANT_TYPE macro for checking
detect : add missing sigorder case
stream : cast memcap checks to uint64_t
ssh : fix out of bounds read in banner parsing
fix seg fault due to wrong sm list access in hscd
dnp3 : use BasicSearch instead of memmem
cuda : fix invalid use of sizeof
defrag : fix reconstruction
counters : don ' t run if no counters have been registered
flowvar : initialize new memory to prevent issues on error handling
detect : free rule group init memory at the end of startup
fix uninit var usage in hhd
reject pcre modifiers U with B
pe : imports : Actually look at FirstThunk if OriginalFirstThunk is zero .
rt : Fix size miscalculation in ivec_reserve
add transition helper to format . h
Fix the build .
removed silly check for >= 0 of a supposedly unsigned value !
http2 : support > 64bit sized uploads
ssh : fix ' left ' may be used uninitialized
get_url_file_name : Fixed crash on OOM on debug build
multi : fix CURLM_STATE_TOOFAST for multi_socket
imap . c : Fixed an incorrect variable reference
hostip . c : return immediately from Curl_resolv_timeout () upon expired timeout .
mime : properly unbind mime structure in curl_mime_free ().
tool_writeout : fixed a buffer read overrun on -- write - out
fix compiler warning
don ' t use curl . haxx . se
gtls : dereferencing NULL pointer
Allow compiling under Android
Fixed debug . c to work again unchanged .
voutf : fix bad arethmetic when outputting warnings to stderr
curl : do bounds check using a double comparison
[ MP ] Prevent a crash in NPC_ChoosePainAnimation when NPCs are knocked down .
Fixed GL error with SDL on startup if not using nvidia card .
fix typo : s / POWOR / POWER /
get rid of strcat
ext / syck / handler . c ( syck_hdlr_add_alias ): add cast to avoid warning .
ext / zlib / zlib . c : initialize return value
comma at the end of enum is a C99ism
string . c : fix one - off bug
thread_sync . c ( condvar_ptr ): reset fork_gen after forking
st . c : fix num_entries
Allow clients to have a resolved hostname of localhost
modules : wow , that was a dumb error .
m_stats : don ' t try to access bl_stats if it doesn ' t exist
blacklist : this didn ' t get committed somehow
opm : properly disable opm on blacklist deletions .
[ git / 2 . 1 regression ] Fix opj_write_tile () failure when numresolutions = 1
Cast to size_t before multiplication
Shut up gcc
kj :: repeat () should support subscript operator .
cmd / 8g : fix possibly uninitialized variable in foptoas .
cmd / 8g : don ' t crash if Prog -> u . branch is nil
cmd / ld : change DWARF encoding for array lengths
runtime : fix 386 , arm build
runtime : don ' t stop bitmap dump at BitsDead
runtime : remove unnecessary ROUND in alg . c
initialise dc variable to satisfy old compilers .
typo fix on function
Fix a memory leak in tls1_mac
x509 : fix double locking problem
Fix a crash in the asn1parse command
Fix a possible crash in dsa_builtin_paramgen2 .
Added error checking for OBJ_create
pkey_gost_ctrl : check for NULL after allocating pctx -> shared_ukm
Avoid a memory leak in OCSP_parse_url ().
Remove an unused variable assignment
GH1141 : Different fix , preferred by Richard .
capi_get_key : check for NULL after allocating key
fix previous modification -- if ssl -> cert is NULL , don ' t follow the pointer .
Updates from 1 . 0 . 0 - stable
fix potential memory leak when allocation fails
Make it possible to disable STORE .
Added a check for malloc fail .
usbutils . c uninitialised usbstat for non - primary mmqs
klondike use a memcpy
Put a lower limit on the nonce increment in cpu mining .
Bugfix : off - by - one in priority calculation
gif_read : Set last_name = NULL unconditionally .
Gifsicle : Free Clp_Parser before exit to clean valgrind report .
policycoreutils : semodule_package : do not fail with an empty fc file
sestatus : free process and file contexts which are checked
libsepol / cil : Destroy classperm list when resetting map perms
libsepol / cil : Destroy classperms list when resetting classpermission
libsepol / cil : cil_reset_classperms_set () should not reset classpermission
fix small memleak in action ()
bugfix : check return value of malloc ()
support force termination in script engine
bugfix : plug a memleak in mmsnmptrapd
fixed potential uninitialzed var access ( highly improbable )
fixed memory leak in worker instance of omelasticsearch
irc : fix parsing of DCC filename
relay : remove unneeded check on variable irc_argv
Skip 0 - length ACL fields
Avoid a double - free when a window size of 0 is specified
Issue 719 : Fix for TALOS - CAN - 154
warc : consume data once read
cast DWORD to int to match % d printf specifier
Check the result of EC_GROUP_new_curve_ * calls
crypto : Enable EDDH all OpenSSL cryptolib over beta version 1 . 1 . 1 - pre8
erts : Search for heart in ports that are alive
erts : Fix beam_load assert
range check - set_default_value
fix crash in limit when user enters invalid regexp
Handle NO response without message properly
proposed fix for clearing labels
Try fixing a possible segmentation fault . I ' m not entirely sure how
imap_quote_string make room for quotes
Try to fix # 302 .
klondike - fix isc uninit warning
fpgautils : add support for 57 . 6 kBd serial
Target should only be 32 bytes copied .
Bugfix : Free filename entry for writing config file when done with it
Bugfix : Missing pool_no parameter to applog for no - stratum - sessionid debug message
syslog requires a facility ... in more than one place
usbutils - which_intinfo not requried
klondike use a memcpy
initialise salen in binresvport_sa
rbd : free owners when rbd_lock_break timeout
ccpp : do not read data from root directories
abrt - hook - ccpp : added a TODO comment . No code changes
ccpp : open file for dump_fd_info with O_EXCL
abrt - gui : change the help accelerator to alt - H
refine addr_header comparison
remove some logs
emacs bindings : added M - d ( kill word )
fixed multiline history
add svn_version () and global var which are
src / wav . c : Fix exif parser bug
src / aiff . c : Fix a buffer read overflow
src / pcm . c : Improve error reporting .
fail on parse error
[ trunk ] added box length inconsistency check
[ trunk ] Import patch from sumatrapdf team . This handle testcase 2977 . pdf . asan . 67 . 2198
Fixing exit - after - terminate when used with bare mode .
backlinks should be updated on rebind
rhbz : die if missing mandatory member
strbuf :: strtrimch () : memmove () reads bytes out of borders
Plugging memory leak in write_unicode
quickfix for metapost
fix minor textbuffer leak , Bug 288 by Toby
scrolling in empty windows crashed
Fixed glib warning with autologging and queries .
mbedtls : print warning if random personalisation fails
Fixed potential misinterpretation of boolean logic
Fixing ecma_create_arguments_object : adding deref for allocated ecma - string
Adding assertion that interpreter doesn ' t receive non - empty normal ecma completion value .
PPP : check PAP AREQ length too , see commit 32118c6
make pigz work on . spl files , otherwise pigz can ' t breaks with kiwi
needed for previous checkin
Remove too early set of auth_user
dnslookup : missing active count inc
Check if auth_user is set .
fix a warning i just introduced
add missing g_free
Free the decision
send . c : if send_ra fails , return - 1
set_interface_var () doesn ' t check interface name and blindly does
remove a debug message ; appears far too often in normal use
[ Bugfix ] Render " Ok " and " Cancel " button correctly when compiled with GNUTLS .
Closing connection after mg_send_digest_auth ()
gcc complains about _MSC_VER being an undefined macro .
Fix an allocation problem in inf_text_fixline_buffer_buffer_insert_text ()
CID 90884 storage_db : Check for bad data type
destroy image and set MemoryError on overflow check failure
initialize unused pointer to prevent freeing without allocating
idevicebackup2 : Only post " sync finished " notification if we sent " sync start " earlier
make sure ' length ' is >= 0 in afc_file_read
fixes depth check in format_string ( can be 0 )
mobilesync : Plugging a memory leak
Fixed an array overflow problem in the JPC decoder .
jpc_t1cod : check whether passno is in range
Avoid a memory leak on non - PAM systems
Add some messages to PAM close session
API : Deprecate __index__ for ndim > 0
BUG : Remove memory leak in np . place
BUG : erroneous use of sprintf
tcti_ldr : load a specific so version
Check the return value of fscanf () when reading a PID file .
[ M68K ] handle -> printer_info need to be check
systemz : fix # 1062
mips : return Fail on assert failure in MipsDisassembler . c
mips : sanity check for input code length of Mips64
fix wrong function name in merge of GPR # 596
cgfs : fix dorealloc ' s batch allocation
pass the UDI up if existant
fix accidently reverted code
rewind wpa file descriptor when uploading
osdep : Fix possible infinite loop ( Closes : # 1582 ).
dracut - install : handle more ldd errors
rgw : fix subuser key name when purging subuser keys
libcephfs : destroy cmount on ceph_shutdown ()
mon : fix bad return test !
mds : drop auth pins before waiting for dir unfreeze
msg : MOSDSubOp has a private destructor
mds : fix uninit field in CInode
mon : Elector : clean ack set on election start
mon : Monitor : get required features from monmap
msg : async : trim already sent for outcoming_bl more efficiently
monmap : clear addr_name map on calculating ranks
msgr : fix operator << for sockaddr_in , too
PG : don ' t compare auth with itself
fragtree_t : fix get_leaves_under ()
filejournal : use :: encode () wrapper func for Transaction
mds : properly free CInode :: ValidationContinuation
librbd : add create_async_context_callback helper
fixed non - root rooted path issue
filejournal : initialize header before writing
rados : statically allocate time
osd : clear INCONSISTENT if scrub detects no errors
mds : shut up about request latency
kclient : check for ENAMETOOLONG in lookup
mds : choose reasonable state after xlock finishes
librbd / internal . cc : close resource leak
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
osd : gracefully exit when create_object_store () returns NULL
osd : avoid polluting peer_info with acting [ 0 ]
mds : handle MMonMap
add stray_prior_path to store path before rename
PG : strays don ' t peer ( only the primary does )
doc : Add usage comment to librbd :: RBD :: AioCompletion
mds : put pending ( not added ) caps in reply caps
msg : initialize footer
osd : fix uninit value in scrub message
rgw : AbortMultipart request returns NoSuchUpload if the meta obj doesn ' t
osd : Update object state after removing watch from object info
rgw : fix flush_read_list () error msg
OSD : lock recovery_wq before debug output on finish_recovery_op
rbd - mirror : gracefully fail if object map is unavailable
osd : fix rare memory leak
mds : update current_parent_since for newly snapshotted directory
rgw : fix use - after - free
rgw : move coroutines out of anonymous namespace
mds : look at active ( not projected ) inode when issuing truncation
msg : MHeartbeat has a private destructor
kclient : fix snap_rwsem link on non - existent cap release
mon : initialize vars
osd_types : add missing osd op flags
correct error of repeatedly adding io_size to off in do_bench_write
osd : drop obsolete ' filestore ' bool option
mds : set bad dirfrag ' s version to 1
msg : MMDSMap has a private destructor
rbd : allow users to remove rbd image when Ceph cluster becomes full .
mds : fix can_rdlock_soon vs xlock on FileLock
mgr : fix OSDMap notifications
filestore : free fiemap used for testing fs capabilities
rgw : bi_list () initialize is_truncated if - ENOENT
kclient : setxattr for virtual xattr is not allowed
add rename check_access
osd : fix trailing + in pg state string rendering
osdc : fix pgls_filter op encoding
osd : EINVAL when rmw_flags is 0
kclient : reset session cap seq # on reconnect
osd : fix build_initial_pg_history
OSDMap : reset osd_primary_affinity shared_ptr when deepish_copy_from
osd : fix leaked OSDMap
Rgw : check the return value when call fe -> run ()
mon : Monitor : drop useless ' todo ' comment
ReplicatedPG : CEPH_OSD_OP_WATCH return - ENOENT if ! obs . exists
auth : delete Exception once it ' s caught ; fixing minor memory leak
mds : zero out the layout in handle_client_setlayout
msg : MClientLease has a private destructor
config option for mon ' s DNS SRV records
Changed name extension of calendar files from " vcf " to " ics ".
bug - fix for bz # 1352432
Fixed malloc . h path on iOS target platform
fix bit - length check in der_decode_raw_bit_string ()
fix FOLDS_FOLD_ADDR_BUK () for remove warning .
jpeg : Fix another possible buffer overrun
Added vim modeline
EbmlElement : don ' t read beyond end of buffer when reading variable length integers
fix memory leak
Add a bounds check to ByteArray :: Get ().
Issue 23433 in oss - fuzz : Heap - buffer - overflow in findNextBorderPixel ()
fixed firejail symlink problem for -- private - bin option
added missing pclose to stop zombies from forming
gitlab # 179 - rsvg_filter_primitive_free (): Check the GStrings for NULL before freeing them
rsvg_characters_impl (): Unref the chars node we just created
dosfslabel : Do not read beyond string length ( Closes : # 709587 ).
fix type mismatch anywhere int != int32 ( Vorne )
Be more careful when calculating sinc () for small arguments
Fixed a GIF decoding bug ( divide by zero )
apps / sipdial : add support for sips : URI scheme
ares : Prevent buffer overlow in ares_parse_a_reply ( CVE - 2017 - 9454 )
Initialize pointer to NULL
buildFHSUserEnv : change to root directory after chroot
made suidrt . c work
util : make sure to close the authfile before returning success
Fixed memory leak when an invalid timezone was found
Flush the bus before exit
regcomp . c : Prevent integer overflow from nested regex quantifiers .
fixed memory leak
squash heap - buffer - overflow , PoC TIFF from Hardik
rfbserver : don ' t leak stack memory to the remote
libvncclient : free vncRec memory in rfbClientCleanup ()
libvncclient : bail out if unix socket name would overflow
fix a used - before - set error in im_vips2dz
Bug 700947 : Add missing ENDTRY opcode in try / catch / finally byte code .
qemu : Add missing lock in qemuProcessHandleMonitorEOF
ofproto : Fix OVS crash when reverting old flows in bundle commit
ipf : release unhandled packets from the batch
Always check the number of coefficients
Fixed possible credentials leaking reported by Alex Birsan .
Changed to using carbon module delegate methods
Log an error when SPIFFS_mount fails
typo in shader
stb_leakcheck : Make stb_leakcheck_free actually free .
Applying InnoDB snapshot
continued fix for reset master
branches / zip : Check for autoinc next value overflow explicitly when offset
fix Bug # 15907954 - SETTING INNODB_MIRRORED_LOG_GROUPS = 2 LEADS TO SERVER
Acquire the trx_sys_t :: lock before traversing the trx list .
Added missing DBUG_RETURN
reverting a patch
Fixed a bug in vis . c
fixed compiler error
stole magnus ' fix for random im test failures from 5 . 1 - maint
Addendum to the fix for bug 43029
correct bug in code
typo in libedit fixed - possible buffer overflow - bug # 4696
fixed bug # 1661 Compiling -- pstack fails with binutils 2 . 13 . 90
pb2 fix : one machine on pb2 didn ' t like TABLE :: map
Tracing test failure : testRedo - n CheckLCPStartsAfterNR T1
blockSend and unblockSend are not declared since
correct parameter of hash_search
Fixed a problem with Innodb_buffer_pool_pages_latched status variable .
branches / zip : ha_innobase :: add_index (): Check for ! innodb_table .
Fixed error in merge
cleaned up lots - o - warnings build
fix github issue # 127
fix aes ctr cast
set correct cert type in CertificateRequest when using ChaCha suite with ECDSA
Fix the WOLFSSL_SEP (-- enable - sep ) build scenario where extCertPoliciesNb is not available .
lib / string . c : add ULL suffix to the constant definition
drm / mgag200 : Constify function pointer structs
sh : Use __GFP_ZERO for dma_generic_alloc_coherent ().
xen / xenbus : fix overflow check in xenbus_file_write ()
ath6kl : Release the memory allocated for the firmware
xilinx_emaclite : Call dev_consume_skb_any instead of dev_kfree_skb .
i2c : xiic : must always write 16 - bit words to TX_FIFO
dlm : sctp_accept_from_sock () can be static
intel - iommu : double kfree ()
drm / i915 : Make disable_cursor_plane similar to commit_cursor_plane .
usb : gadget : f_subset : fix missing unlock on error in geth_alloc ()
fs / binfmt_misc . c : add terminating newline to / proc / sys / fs / binfmt_misc / status
drivers / net / skfp : if ! capable ( CAP_NET_ADMIN ): inverted logic
sched / deadline : Skip in switched_to_dl () if task is current
[ SCSI ] fcoe : fix missing error check in call to fcoe_if_init
[ SCSI ] mpt fusion : make mptsas_smp_handler update resid
wlcore : fix unsafe dereference of the wlvif
mm : highmem : export kmap_to_page for modules
[ PATCH ] auerswald . c : % zd for size_t
[ IPSEC ]: Fix inetpeer leak in ipv4 xfrm dst entries .
efikasb / mx : fix usbh1 initialisation
staging : rtl8192e : don ' t just print an error and continue
e1000 : only enable TSO6 via ethtool when using correct hardware
userns : don ' t leak root user
arm64 : Align CMA sizes to PAGE_SIZE
staging : clocking - wizard : check for a valid clk_name pointer
[ IPV4 ]: ipv4_is_lbcast () misannotations
mtd : nand_bbt : set the smallest size of bbt table
drm / radeon / kms / evergreen : No EnableYUV table
KVM : MMU : fix shrinking page from the empty mmu
[ PATCH ] it87 : Fix oops on removal
staging : comedi : pcmuio : fix the last > 80 char line warning
drm : work around EDIDs with bad htotal / vtotal values
KVM : PPC : e500 : Eliminate preempt_disable in local_sid_destroy_all
s390 / con3270 : testing return kzalloc retval
xen / pciback : Check PF instead of VF for PCI_COMMAND_MEMORY
loop : Update mtime when writing using aops
xen - gntalloc : integer overflow in gntalloc_ioctl_alloc ()
mtd : orion_nand : Fix build failure caused by typo
iwlwifi : fix kernel oops when ucode DMA memory allocation failure
bridge : cleanup : remove unneed check
crypto : atmel - sha - add new version
regmap : Don ' t attempt block writes when syncing cache on single_rw devices
af_rxrpc : Handle frames delivered from another VM
drm / vmwgfx : Fix kernel NULL pointer dereference on older hardware
drm : fixup improper cast .
staging : media : lirc : Use pr_ * instead of printk
staging : dwc2 : when dma is disabled , clear hcd -> self . uses_dma
[ SCSI ] zfcp : prevent fc_remote_port_delete calls for unregistered rport
i2c / ds1374 : Check workqueue creation status
mmc : dw_mmc : Don ' t print data errors
rtc : rtc - twl : Switch to using threaded irq
pinctrl : fix a minor harmless typo
i4l : leak in eicon / idifunc . c
[ PATCH ] fix Bug 4395 : modprobe bttv freezes the computer
staging : android : logger : enforce GID and CAP check on log flush
[ SCSI ] qla2xxx : Don ' t hardcode fw_transfer_size for ISP2 [ 45 ] xx parts .
dma : of - dma . c : fix memory leakage
bnx2 : Zero out context memory for 5709 .
staging : sm750fb : fix style for if clause
[ ALSA ] Fix possible deadlocks in sequencer at removal of ports
mm : fix NULL ptr deref when walking hugepages
libceph : Remove spurious kunmap () of the zero page
ext4 : online defrag is not supported for journaled files
[ IA64 ] Correct pernodesize calculation .
ceph : add missing locking to protect i_snap_realm_item during split
s390 / hugepages : clear page table for sw large page emulation
iwlwifi : mvm : allow RSSI compensation
ext4 : Make sure the MOVE_EXT ioctl can ' t overwrite append - only files
igb : limit EEPROM access
xfrm : Force SA to be lookup again if SA in acquire state
[ PATCH ] IB : Initialize qp -> wait
KVM : x86 emulator : fix lea to really get the effective address
posix - timer : Don ' t call idr_find () with out - of - range ID
tracepoints : fix disable
ASoC : wm8994 : Move wm_hubs callback before we start ramping VMID
gpio - 74x164 : Fix buffer allocation size
usb : Do not allow usb_alloc_streams on unconfigured devices
[ ARM ] 5072 / 1 : corgi_pm : don ' t register devices on non - corgi machines
cfg80211 : Process all pending regulatory requests / hints
[ PATCH ] pcmcia : socket . functions starts with 1
rt2x00 : Add RT539b chipset support
x86 : Fix panic when handling " mem ={ invalid }" param
ath10k : add module parameter values to the pci info print
mtip32xx : remove unneeded variable in mtip_cmd_timeout ()
[ media ] davinci : vpbe : fix build error when CONFIG_VIDEO_ADV_DEBUG is enabled
[ PATCH ] bug in futex unqueue_me
x86 / hpet : Check for irq == 0 when allocating hpet MSI interrupts
ceph : exclude snapdir from readdir results
xfs_file_last_byte () needs to acquire ilock
staging : vt6655 : check ieee80211_bss_conf bssid not NULL
ath6kl : fix memory leak in ath6kl_fwlog_block_read ()
[ NETFILTER ]: bridge : fix double POSTROUTING hook invocation
arch / powerpc / sysdev / ehv_pic . c : add missing kfree
mm : Adjust do_pages_stat () so gcc can see copy_from_user () is safe
sch_gred : should not use GFP_KERNEL while holding a spinlock
drm : Fix a bug in the range manager .
brcmfmac : remove ( ab ) use of NL80211_NUM_ACS
fuse : invalidate target of rename
bridge : netfilter : orphan skb before invoking ip netfilter hooks
staging : comedi : fix typo in error message
brcm80211 : smac : set interface down on reset
staging : lustre : info leak in lnet_ping ()
mm : vmalloc search restart fix
drm / radeon / vm : don ' t attempt to update ptes if ib allocation fails
mfd : Handle missing WM8350 platform data
alpha : long constant
sparc64 : Force the execute bit in OpenFirmware ' s translation entries .
[ SCSI ] bfa : off by one in bfa_ioc_mbox_isr ()
staging : vt6655 : Use NULL instead of 0
drivers / rtc / rtc - pcf8563 . c : fix pcf8563_irq () error return value
[ IPV6 ]: Fix ipv6 GSO payload length
pm2fb : reset transparency settings
[ IA64 ] Fix trap # 45 handling
staging : vt6655 : dpc . c : replace memcpy () by ether_addr_copy () using coccinelle
drivers / net / phy / mdio_bus . c : call put_device on device_register () failure
Staging : cxt1e1 : use netdev_priv to fix build
KVM : MCE : Add MCG_SER_P into KVM_MCE_CAP_SUPPORTED
[ ALSA ] Add missing compat ioctls for ALSA control API
Staging : android : timed_gpio : Properly discard invalid timeout values .
pinctrl : sirf : Remove gpiochip on failure cases
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
xen / p2m : Fix one - off error in checking the P2M tree directory .
ARM : SPEAr : conditionalize l2x0 support
[ TG3 ]: Update version to 3 . 82 .
lockdep : fix debug_show_all_locks ()
iio : ti_am335x : Remove redundant call to iio_sw_buffer_preenable ()
hwmon : ( hih6130 ) Fix missing hih6130 -> write_length setting
[ media ] tvp514x : Fix double free
sparc32 : Need to close openned RTC device just like sparc64 .
fs : fix data - loss on error
hwmon : ( w83793 ) Fix vrm write operation
mtd : nand : update mtd_to_nand ()
dmaengine : bcm2835 - dma : Fix memory leak when stopping a running transfer
brcmsmac : handle non pci in ai_deviceremoved ()
ecryptfs : avoid to access NULL pointer when write metadata in xattr
tuntap : set SOCK_ZEROCOPY flag during open
phy layer : fix genphy_setup_forced ( don ' t reset )
staging : csr : remove unneeded call to memset ().
staging : rtl8723au : Fix the indentation of two lines
bna : Check for NULL before deref in bnad_cb_tx_cleanup
[ POWERPC ] arch / powerpc / sysdev / qe_lib of_node_get cleanup
[ BNX2 ]: Update version to 1 . 6 . 8 .
xfs : Fix quota type in quota structures when reusing quota file
amdkfd : use atomic allocations within srcu callbacks
stmmac : fix check for phydev being open
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
HID : multitouch : force retrieving of Win8 signature blob
spectra : Don ' t overwrite nand_debug_level . It ' s a module param
cifs : Read buffer overflow
mfd : Use devm_ * APIs for mc13xxx : remove leftover kfree
bridge : mcast snooping , fix length check of snooped MLDv1 / 2
[ PATCH ] sys_ioprio_set : minor do_each_thread + break fix
staging : lustre : Remove unnecessary parentheses .
[ BNX2 ]: check return of dev_alloc_skb in bnx2_test_loopback
usb : gadget : storage : release superspeed descriptors .
tcp : remove unnecessary tcp_sk assignment .
tracing : Have trace buffer point back to trace_array
[ ALSA ] soc - Report errors from snd_soc_dapm_set_endpoint ()
ALSA : hda - Remove superfluous pcm NULL check
tools / firewire : nosy - dump : work around segfault in decode_fcp
ath9k : Send AUTHORIZED event only for station mode
drivers / net / eql . c : prevent reading uninitialized stack memory
[ MIPS ] Sibyte : Fix name of clocksource .
fsnotify : kzalloc fsnotify groups
KVM : VMX : Fix pending NMI - vs .- IRQ race for user space irqchip
mac80211 : fix potentially redundant skb data copying
HID : egalax : Use kzalloc
mmc : sdhci - esdhc - imx : change default watermark level and burst length
drm / i915 : Drop any active reference before unbinding
bpf : fix BPF_MAP_LOOKUP_ELEM command return code
mv643xx_eth : prevent interrupt storm on ifconfig down
USB : atmel_usba_udc : fix freeing irq in usba_udc_remove ()
ath9k : make sure main_rssi is positive
Staging : crystalhd : use vfree () instead of kfree ()
irda : Fix irda_getname () leak
svcrdma : RDMA support not yet compatible with RPC6
memcg : fix leak of accounting at failure path of hugepage collapsing
mfd : add support for WM8351 revision B
iscsi class : fix get_host_stats error handling
[ MIPS ] add io_map_base to pci_controller on Cobalt
drivers / net / cpmac . c : Correct code taking the size of a pointer
usb : isp1760 : check for null return from kzalloc
tcp : tcp_disconnect () should clear window_clamp
[ media ] v4l : vsp1 : Fix VI6_DISP_IRQ_ENB_LNEE macro
[ SCSI ] fc_transport : Check portstates before invoking target scan
sh : Zero - out coherent buffer in consistent_alloc ().
usb : gadget : uvc : memory leak in uvcg_frame_make ()
iio : kfifo_buf improve error handling in read_first_n .
drm / nouveau : Do not leak client objects
ext4 : use the correct error exit path in ext4_init_inode_table ()
i386 : arch / x86 / math - emu / fpu_entry . c warning fix
[ ATA ] Add named constant for ATAPI command DEVICE RESET
dlm : avoid unnecessary search in search_rsb
ncpfs : successful rename () should invalidate caches for parents
[ SCSI ] ipr : Bump version
[ MIPS ] Fix return type of gt64120_irq .
clk : tegra : Use definition for pll_u override bit
ceph : fscache : Update object store limit after file writing
iwlwifi : unlock on error path
ath5k : free sc -> ah in ath5k_deinit_softc
fib6 : use FIB_LOOKUP_NOREF in fib6_rule_lookup ()
KVM : Fix coalesced interrupt reporting in IOAPIC
ceph : drop CAP_LINK_SHARED when sending " link " request to MDS
perf record : Use sw counter only if hw pmu is not detected
USB : xhci : No GFP_KERNEL in block error handling
Staging : wlan - ng : fix style in p80211netdev . c
perf / x86 / amd : Fix RDPMC index calculation for AMD family 15h
[ PATCH ] uml : const a variable
[ media ] hdpvr : Fix memory leak
Staging : vt6655 : add missing free_netdev () on error in hostap_enable_hostapd ()
x86 : Use string instruction memcpy on AMD Fam11h
x86 : fix printout ugliness in cpu info printk
char / tpm : Fix unitialized usage of data buffer
lib : correct 842 decompress for 32 bit
staging : comedi : cb_pcidas : check for failure of subdev_8255_init
viafb : reduce I2C timeout and delay
PCI : spear : Fix Section mismatch compilation warning for probe ()
cxl : Plug irq_bitmap getting leaked in cxl_context
pasemi_mac : Access iph -> tot_len with correct endianness
drm / amdkfd : unmap VMID <--> PASID when relesing VMID ( non - HWS )
x86 , efi : Check table header length in efi_bgrt_init ()
drm / i915 : Wrap - EIO send - vblank event for failed pageflip in spinlock
omap3 : Fix booting if package is uninitialized
[ PATCH ] frv : fix uninitialised variable in serverworks driver
mm : memblock : fix wrong memmove size in memblock_merge_regions ()
Input : ab8500 - ponkey - fix IRQ freeing in error path
ixgbe : reinit_locked () should be called with rtnl_lock
ipv6 : udp : fix the wrong headroom check
misc : genwqe : fix return value check in genwqe_device_create ()
md : check the return of mddev_find ()
ARM : shmobile : r8a7778 : add MMCIF clock support for DT
ARM : mach - imx : mach - imx6q : Fix sparse warnings
UIO : uio_pdrv : fix memory leak
i810fb : the pseudo_palette is only 16 elements long
mac80211 : pass AP vif pointer for VLANs
Btrfs : fix false positive in check - integrity on unmount
[ PATCH ] i386 : Terminate backtrace fallback early if unwinder stack pointer is zero
[ ALSA ] system timer : clear correction value when timer stops
e1000e : potentially incorrect return for e1000e_setup_fiber_serdes_link
adm8211 : fix cfg80211 band API conversion
PCI : fix bridge I / O window bus - to - resource conversion
thinkpad - acpi : restrict procfs count value to sane upper limit
ipv6 : Export nf_ct_frag6_gather ()
drm / i915 : fix lane bandwidth capping for DP 1 . 2 sinks
ath5k : add missing include for debug code
[ PATCH ] m68k : isa_ { type , sex } should be exported
Staging : speakup : free after input_unregister_device ()
staging : zcache : fix possible sleep under lock
[ PATCH ] x86_64 : free_bootmem_node needs __pa in allocate_aperture
ima : Do not free ' entry ' before it is initialized
ASoC : wm8731 : fix wm8731_check_osc () connected condition
[ media ] V4L : mt9t112 : use after free in mt9t112_probe ()
perf_events : Fix invalid pointer when pid is invalid
Btrfs : Fix balance panic
ALSA : Fix a Oops bug in omap soc driver .
net : make ipv6 PKTINFO honour freebind
[ SCSI ] iscsi : redirect fix
crypto : omap - sham - fix concurrent sha1 calculations
batman - adv : Only put gw_node list reference when removed
perf session : Skip event correctly for unknown id / machine
scsi : don ' t add scsi_device if its already visible
ring - buffer : Fix first commit on sub - buffer having non - zero delta
[ PATCH ] ip_output : account for fraggap when checking to add trailer_len
[ PATCH ] KVM : Avoid oom on cr3 switch
drbd : fix if (); found by kbuild test robot
ipv6 : fix headroom calculation in udp6_ufo_fragment
leds : 88pm860x : add missing of_node_put
virtio_net : zero is an invald queue_pairs number
ARM : mx51 : Fix the address space length for SSI
rtnetlink : Fix VF IFLA policy
IPoIB : Free child interfaces properly
wan : wanxl : add pci_disable_device in case of error
drm / msm : fix memory leak
net : make all team port device link events urgent
[ SCSI ] libfc : Fix e_d_tov ns -> ms scaling factor in PLOGI response .
md : avoid possible spinning md thread at shutdown .
usb - storage : Add 090c : 1000 to unusal - devs
staging : ozwpan : prevent overflow in oz_cdev_write ()
ath9k_hw : add default chainmask for AR9462
spi : s3c64xx : Zero dma_slave_config struct in prepare_dma ()
btrfs : don ' t BUG_ON allocation errors in btrfs_drop_snapshot
stmmac : fix csr clock divisor for 300MHz
iucv : fix memory leak in cpu hotplug error path .
scm : Capture the full credentials of the scm sender
ASoC : generic - dmaengine - pcm : use a more common dma name
iwlwifi : fix rx signal quality reporting in dmesg
[ media ] gspca - ov519 : Fix a regression for ovfx2 webcams
drm / nouveau / core : drop the pointer value in debug printk output
atmel_usba_udc : Add missing kfree () in usba_udc_remove ()
sunrpc : copy scope ID in __rpc_copy_addr6
tg3 : remove open - coded skb_cow_head .
perf stat : Using init_stats instead of memset
dm snapshot : fix a possible invalid memory access on unload
[ ALSA ] soc - 0 . 13 ASoC DAPM bug fix for unnamed streams
ext3 : explicitly remove inode from orphan list after failed direct io
af_iucv : support ancillary data with HS transport
mmc : card . h : Use NULL instead of 0 for END_FIXUP
[ media ] s5p - fimc : Fix return value of __fimc_md_create_flite_source_links ()
arm64 : drop unnecessary cache + tlb maintenance
drm : missing drm_vm . c changes for consistent maps
[ XFS ] Wait for all I / O on truncate to zero file size
ixgbe : Return PCI_ERS_RESULT_DISCONNECT when bus is disabled
bnx2x : fix DMAE timeout according to hw specifications
[ MIPS ] Sibyte : Fix interrupt timer off by one bug .
usb : message : Fixed parenthesis error in sizeof function .
KVM : VMX : Stop invalid guest state emulation on pending event
Staging : Fix leak in drivers / staging / at76_usb . c
mtd : cmdlinepart : allow small partitions
drm / i915 : switch to default context on idle
fuse : fix d_revalidate oopsen on NFS exports
Btrfs : Don ' t case unsigned long to int in bio submission
ext4 : Fix update of mtime and ctime on rename
gpio / ep93xx : Use irq_set_handler_locked ()
sctp : fix possible seqlock seadlock in sctp_packet_transmit ()
spi : img - spfi : Enable controller before starting TX DMA
sh64 : re - add the __strnlen_user () prototype
drm / i915 : Don ' t kick - off hangcheck after a DRI interrupt
mfd : Check for twl4030 - madc NULL pointer
ASoC : core : Fix dai_link dereference .
rocker : print switch ID consistent with phys_switch_id sysfs node
perf tools : Disallow PMU events intel_pt and intel_bts until there is support
include / asm - x86 / setup . h : checkpatch cleanups - formatting only
md : allow a partially recovered device to be hot - added to an array .
HID : wacom : Do not repeatedly attempt to set device mode on error
sched : Disallow sched_attr :: sched_policy < 0
video : udlfb : Use NULL instead of 0
perf annotate browser : Fix behaviour of Shift - Tab with nothing focussed
f2fs : don ' t need to sync node page at every time
perf ui hist browser : Fix segfault on ' a ' for annotate
[ ALSA ] Fix substream to check in PCM drain
[ PATCH ] key : plug request_key_auth memleak
tracing : Fix event format export
drivers / char / hpet . c : fix information leak to userland
ext4 : check free block counters in ext4_mb_find_by_goal
gpio : adp5588 : remove unnecessary OOM messages
drm : Fix DRM_FORCE_ON_DIGITAL use
btrfs : quota : Update quota tree after qgroup relationship change .
isdn : avoid copying too long drvid
ath10k : free wmi mgmt event skb when parsing fails
drm : drm_ioctl () should zero - init extra data
[ media ] em28xx : fix em28xx - input removal
perf session : Don ' t call dump_sample () when evsel is NULL
mfd : t7l66 MMC platform data
dm : raid fix write_mostly arg validation
ARM : at91 : remove empty at91_init_serial function
arch / powerpc / sysdev / qe_lib / qe . c : Add of_node_put to avoid memory leak
r6040 : use ETH_ZLEN instead of MISR for SKB length checking
usb : gadget : fusb300_udc : fix error return code in fusb300_probe ()
ASoC : max9867 : silence and array overflow warning
KVM : Fix buffer overflow in kvm_set_irq ()
ARM : PL08x : prevent dma_set_runtime_config () reconfiguring memcpy channels
Btrfs : btrfs_add_ordered_operation : Fix last modified transaction comparison .
PCI : eliminate double kfree in intel - iommu initialization
ASoC : Staticise pxa2xx_pcm_ops
[ PATCH ] libata : kill illegal kfree ( id )
mtd : mxc_nand : set owner field to prevent module unloading when in use
ASoC : arizona : Store the DAI clock ID when setting
x86 / PCI : move final fixups from __init to __devinit
ARM : Fix warning : unused variable ' highmem '
tcp : skb_mstamp_after helper
cris : fix potential NULL - pointer dereference
mmc : card : Avoid null pointer dereference
dmaengine : ste_dma40 : fix error return code
ARM : OMAP2 +: only WARN if a GPMC child probe function fail
ASoC : wm8995 : Return - EINVAL if device ID mismatch
rcar - hpbdma : add max transfer size
firewire : fix failure path in ohci_enable_phys_dma
[ SCTP ]: Validate the parameter length in HB - ACK chunk .
usb : m66592 - udc : Fixed bufnum of Bulk
USB : serial : fix up bug with missing {}
i40evf : Add more info to interrupt vector names
netrom : Fix nr_getname () leak
mac80211 : add status_driver_data array to ieee80211_tx_info
USB : whiteheat : fix memory leak in error path
ath9k : fix massive rx packet loss issue
sched : add sanity check in partition_sched_domains ()
wl12xx : update PLT initialization for new firmware
drivers / net / ax88796 . c : Return error code in failure
ext4 : error proc entry creation when the fs / ext4 is not correctly created
iwlwifi : mvm : initialize the cur_ucode upon boot
staging : wilc1000 : off by one in wilc_wfi_cfg80211_mgmt_types
firewire : cdev : fix another memory leak in an error path
pinctrl : lpc18xx : fix schmitt trigger setup
[ PATCH ] sysfs : fix a kobject leak in sysfs_add_link on the error path
drm_vblank_get : don ' t WARN_ON in case vblanks are not initialized
usb : gadget : f_printer : actually limit the number of instances
ath9k : Clear HT info on starting the driver
[ media ] media : davinci_vpfe : clear the output_specs
staging : rts_pstor : Complete scanning_done variable
pata_octeon_cf : fix broken build
gpio / samsung : Complain loudly if we don ' t know the SoC
af_unix : implement socket filter
Btrfs : fix extent_buffer leak during tree log replay
vmw_pvscsi : fixup tagging
[ PATCH ] manage - jbd - its - own - slab fix
drm / amdgpu : fix userptr flags check
leds : pca963x : Add missing initialiation of struct led_info . flags
f2fs : persist system . advise into on - disk inode
[ PATCH ] fs / compat . c : fix ' if ( a |= b )' typo
usb : musb : blackfin : call gpio_free () on error path in musb_platform_init ()
Btrfs : fix memory leak in start_transaction ()
ext4 : use stream - alloc when mb_group_prealloc set to zero
ACPICA : Fix to allow aliases within ASL namepaths
drivers / char / applicom . c : fix information leak to userland
[ media ] media : rcar_vin : Reject videobufs that are too small for current format
Staging : lustre : osc : Remove null check before kfree
hsi : nokia - modem : fix uninitialized device pointer
drm / nouveau / fifo / nvc0 : bash some magic reg to make uevent interrupt work
ext4 : add missing space to ext4_msg output in ext4_fill_super ()
Btrfs : remove dead code
ata : fix sparse warning in libata . h
ALSA : hda / realtek - Add more entry for enable HP mute led
x86 / nmi : export local_touch_nmi () symbol for modules
ASoC : Intel : mfld - pcm : don ' t call trigger ops to DSP for internal streams
ALSA : hda - check NULL pointer when creating SPDIF controls
wl12xx : set max_sched_scan_ie_len correctly
alloc_percpu : fix UP build
arm / arm64 : KVM : fix missing unlock on error in kvm_vgic_create ()
[ media ] solo6x10 : Fix solo_eeprom_read retval type
dmaengine : k3 : Remove chancnt affectations
ALSA : asihpi - Fix potential Oops in snd_asihpi_cmode_info ()
ARM : footbridge : fix overlapping PCI mappings
mwifiex : fix Coverity SCAN CID 709078 : Resource leak ( RESOURCE_LEAK )
simple_xattr : permit 0 - size extended attributes
x86 , mrst : Don ' t blindly access extended config space
rt2x00 : Fix memleak in tx () path
ssb : Fix error when V1 SPROM extraction is forced
bnx2x : Prevent " Unknown MF " print in SF mode
drm : Revert " drm : Reject modes with more than 1 stereo flags set "
[ SCSI ] pmcraid : reject negative request size
ARM : tegra : fix U16 divider range check
bonding : bond_xmit_roundrobin () fix
drivers / staging / hv / blkvsc_drv . c : eliminate NULL pointer dereference
drm / i915 : Initialise g4x watermarks for disabled pipes
staging : gdm72xx : Condense two statements into one to improve code readability .
be2net : initialize netdev -> perm_addr
dma / timberdale : simplify conditional
sched / balancing : Fix ' local -> avg_load > sds -> avg_load ' case in calculate_imbalance ()
KVM : Really remove a slot when a user ask us so
mlx4_en : Fix read buffer overflow in mlx4_en_complete_rx_desc ()
drm / i915 : Check the framebuffer offset
amd - iommu : set evt_buf_size correctly
rt2x00 : Fix memleak when RTS / CTS fails
rt2x00 : Shortcut link state updates when not operating as STA
sched : Try to deal with low capacity , fix update_sd_power_savings_stats ()
ipvs : WRR scheduler does not need GFP_ATOMIC allocation
[ media ] cx25821 - audio - upstream . c : Move operators
sfc : Read - to - clear LM87 alarm / interrupt status at start of day
iscsi - target : return - EINVAL on oversized configfs parameter
[ media ] marvell - cam : Don ' t signal multiple frame completions in scatter / gather mode
hwmon : ( f75375s ) Disable setting DC fan control mode for F75373
HWPOISON : Fix page count leak in hwpoison late kill in do_swap_page
[ PATCH ] IB : fix CM use - after - free
ath5k : validate mode in ath5k_ani_init () before trying to set it
perf tools : Fix new_term () missing free on error path
dlm : tcp_connect_to_sock should check for - EINVAL , not EINVAL
ath9k : set MAC version for AR9550
i386 : Handle P6s without performance counters in nmi watchdog
ath9k : check for NULL rate when using ieee80211_get_rts_cts_rate ()
ixgbe : support software timestamping
drm / i915 : Reject opening of pipe crc files for invalid pipes
vfio : fix crash on rmmod
rcu : Export rcu_scheduler_active
drm / i915 : fix inconsistent brightness after resume
vxge : Update driver_config -> vpath_per_dev for each function in probe .
Btrfs : process mount options on mount - o remount ,
perf evsel : Fix uninitialized memory access to struct perf_sample
UBI : init_volumes : Ignore volumes with no LEBs
ceph : avoid null deref in osd request error path
drm / i915 : Fix missing return warning for ! CONFIG_DEBUGFS
sh_eth : check TSU registers ioremap () error
iwlagn : overwrite EEPROM chain setting for 6250 devices
[ PATCH ] selinux : Fix MLS compatibility off - by - one bug
Input : turbografx - fix potential out of bound access
xprtrdma : Back off rkey when FAST_REG_MR fails
[ media ] cx24120 : Add missing command to cx24120_check_cmd
ehca : convert cpu notifier to return encapsulate errno value
[ POWERPC ] EEH : Work with device endpoint , always
mac80211 : fix rx -> key NULL pointer dereference in promiscuous mode
Input : ALPS - fix command mode check
USB : cyberjack : fix buggy integer overflow test
x86 , tsc : Fix cpufreq lockup
[ media ] fc2580 : silence uninitialized variable warning
ARM64 : early_printk : Fix check for CONFIG_ARM64_64K_PAGES
[ media ] v4l : vb2 - dma - contig : align buffer size to PAGE_SIZE
pinctrl : handle zero found dt pinconfig properties better
leak in do_ubd_request
tcp_yeah : don ' t set ssthresh below 2
mac80211 : use RCU read locks for sta_info_get
/ proc / vmcore : fix seeking
mm : fix lost kswapd wakeup in kswapd_stop ()
mtd : mtd_blkdevs : don ' t increase ' open ' count on error path
[ PATCH ] zd1211rw : Add ID for Belkin F5D7050 v4000
drm / radeon : don ' t allow device to be opened if powered down
usb : dwc2 : gadget : ignore stall check for ep0
mnt : Carefully set CL_UNPRIVILEGED in clone_mnt
Input : gamecon - clear unused function pointers
ipv4 : fix nexthop attlen check in fib_nh_match
KVM : async_pf : Add missing call for async page present
[ Bug 760 ] Fix setting WEP key in monitor mode causes IV lost .
dmaengine : ste_dma40 : Declare memcpy config as static
mmc : atmel - mci : r / w proof capability only available since v2xx
gpio : Fix range check in of_gpio_simple_xlate ()
kvm / x86 : Remove Hyper - V SynIC timer stopping
USB : chaoskey read offset bug
crypto : mxs - dcp - fix type of ret for wait_for_completion_timeout
um : Ensure that a stub page cannot get unmapped
mwl8k : fix NULL pointer dereference on receive out - of - memory
drm / amdgpu : fix bug of vm_bo_map ( v2 )
sparc : semtimedop () unreachable due to comparison error
ARM : EXYNOS : Recognize Samsung MFC v8 devices
at86rf230 : cleanup and squash stack variable
[ PATCH ] Fix RLIMIT_RTPRIO breakage
drivers / video / backlight / adp8870_bl . c : add missed props . type conversion
drivers / staging / lustre : Fix set - but - unused whinge .
tracing : stop stack trace on first empty entry
tty : moxa : fix bit test in moxa_start ()
tg3 : 5785 : Set port mode to MII when link down
USB : rtl8150_disconnect () needs tasklet_kill ()
drm / vmwgfx : Fix uninitialized return in vmw_cotable_unbind ()
staging : vt6655 : buffer overflow in ioctl
sh_eth : Fix memleak in sh_mdio_release
ext3 : Truncate allocated blocks if direct IO write fails to update i_size
Staging : comedi : integer overflow in do_insnlist_ioctl ()
[ SCSI ] mpt2sas : remove extraneous sas_log_info messages
hwmon / smsc47b397 : Don ' t report missing fans as spinning at 82 RPM
libata - sff : Don ' t call bmdma_stop on non DMA capable controllers
cfg80211 : DFS check dfs_region before usage
mmc : mxs - mmc : fix error return code in mxs_mmc_probe ()
tridentfb : fix hang on Blade3D with CONFIG_CC_OPTIMIZE_FOR_SIZE
ASoC : Fix typo in 24 . 576MHz rate in WM5100
timer : Convert kmalloc_node (... GFP_ZERO ...) to kzalloc_node (...)
microblaze : rt_sigreturn is too trigger - happy about sigaltstack errors
[ SCSI ] iscsi : nodelay fix
irq : check domain hwirq range for DT translate
[ TCP ]: Never TSO defer under periods of congestion .
ALSA : compress_core : integer overflow in snd_compr_allocate_buffer ()
slob : fix free block merging at head of subpage
mm : vmalloc allocator off by one
drm / i915 : The sprite scaler on Ironlake also support YUV planes
iwlwifi : mvm : make sure AP is operating for ToF
[ PATCH ] eCryptfs : dput () lower d_parent on rename
fbcon : bgcolor fix
mac80211 : make sure hw_key exists before checking its flags
[ IA64 ] Hole in IA64 TLB flushing from system threads
kmemleak : Fix the kmemleak tracking of the percpu areas with ! SMP
[ PATCH ] moxa serial : add proper capability check
drm / exynos : check crtc ' s dpms mode at SetCrtc
[ media ] dvb_demux : fix deadlock in dmx_section_feed_release_filter ()
USB : serial : add special case for processing of empty read urbs
be2iscsi : add an missing goto in error path
Staging : unisys : visorinput : Remove unused code
[ SCSI ] iscsi_boot_sysfs : Fix a memory leak in iscsi_boot_destroy_kset ()
drm / i915 : mark display port DPMS state as ' ON ' when enabling output
kexec : fix omitting offset in extended crashkernel syntax
libceph : remove redundant declaration
[ PATCH ] IB / mthca : Fix memory leak on error path
ASoC : rsnd : rsnd_dai_connect () returns error if it connect to existing mod
UBI : Fastmap : Notify user in case of an ubi_update_fastmap () failure
mtip32xx : Set queue bounce limit
KVM : MMU : Fix cleaning up the shadow page allocation cache
blkcg : don ' t call into policy draining if root_blkg is already gone
dm snapshot : lock snapshot while supplying status
efi : rtc - efi : Mark UIE as unsupported
x86 / PCI : make sure _PAGE_IOMAP it set on pci mappings
watchdog : gef_wdt . c : add missing remove callback
svcrdma : Free context on post_recv error in send_reply
ovl : fix open in stacked overlay
cgroup : use release_agent_path_lock in cgroup_release_agent_show ()
clockevents : export clockevents_unbind_device instead of clockevents_unbind
ath9k_htc : check for underflow in ath9k_htc_rx_msg ()
irda : off by one
ixgbe : check for vfs outside of sriov_num_vfs before dereference
tpm : Do not dereference NULL pointer if acpi_os_map_memory () fails .
sdio : kick the interrupt thread upon a resume
staging : binder : fix usage of uninit scalar in binder_transaction ()
ipv6 - multicast : Fix memory leak in input path .
OpenRISC : Export dump_stack ()
x86 / irq : Fix move_irq_desc () for nodes without ram
ftdi_sio : fix kref leak
[ SCSI ] be2iscsi : fix bad if expression
wl1271 : exit ELP mode when setting enabled rates in tx
staging : ft1000 : dont cast void * from kmalloc ()
arm : kvm : fix CPU hotplug
gianfar : Check if phydev present on ethtool - A
intel_pmic_gpio : fix off - by - one value range checking
rt2x00 : disable TKIP on USB
ide : unexport ide_hwifs []
[ PATCH ] pcnet32 : remove incorrect pcnet32_free_ring
Staging : sep : fix time handler
perf hists : Don ' t consider filtered entries when calculating column widths
mm : migrate : hugetlb : putback destination hugepage to active list
xhci : fix off by one error in TRB DMA address boundary check
sctp : Clear fast_recovery on the transport when T3 timer expires .
drm / cirrus : Constify function pointer structs
fs / ramfs / file - nommu . c : replace count * size kzalloc by kcalloc
btrfs : reada : move reada_extent_put to place after __readahead_hook ()
ethtool : time to blink provided in seconds not jiffies
[ SCSI ] Return NEEDS_RETRY for eh commands with status BUSY
econet : Fix econet_getname () leak
hugetlbfs : fix i_blocks accounting
um : Missing pipe handling
ath : Fix NULL ptr dereference in ath_reg_apply_world_flags
[ MTD ] [ OneNAND ] omap2 : panic_write may be in an interrupt context
xen : correct error for building p2m list on 32 bits
VFIO : platform : clear IRQ_NOAUTOEN when de - assigning the IRQ
staging : iio : adc : max1363 : prevent buffer overflow
KVM : Force vmexit with virtual interrupt delivery
xhci : fix sparse warning in xhci - trace . h
dm - raid : silence compiler warning on rebuilds_per_group .
perf : Fix NULL deref
af_packet : remove unnecessary BUG_ON () in tpacket_destruct_skb
crypto : ccm - Fix handling of null assoc data
staging : vt6656 : turn radio off after mac registered .
perf tools : Fix TUI helpline output
ieee1394 : sbp2 : prepare for s / g chaining
[ SCSI ] scsi_lib : Set the device state from transport - offline to running
ixgbe : fix for race with 8259 ( 8 | 9 ) during shutdown
perfcounters : fix refcounting bug
x86 / smpboot : Move data structure to its primary usage scope
iwlwifi : mvm : don ' t allow sched scans without matches to be started
Input : wistron_btns - fix a memory leak in wb_module_init error path
cfq - iosched : slice offset should take ioprio into account
staging : iio : adis16400 : Remove samplerate_available attribute
staging : iio : iio_ring_rip_outer return immediately if rip_lots returns <= 0
tg3 : Turn off ASF " driver alive " heartbeats for APE
[ media ] zoran : fix compiler warning
intel_sst : ignore IRQ when suspended
[ POWERPC ] Fix harmless typo
dm era : check for a non - NULL metadata object before closing it
[ XFRM ]: Fix missed error setting in xfrm4_policy . c
[ SCSI ] scsi_transport_iscsi : make priv_sess file writeable only by root
KVM : SVM : Advance instruction pointer in dr_intercept
dt : protect against NULL matches passed to of_match_node ()
pxa3xx_nand : reset read buffer before reading
UDF : Close small mem leak in udf_find_entry ()
sctp : add a checking for sctp_sysctl_net_register
tools : iio : iio_event_monitor : check if event is fully read
ASoC : Revert " ASoC : mc13783 : Provide codec -> control_data "
staging : iio : isl29018 : fix sparse warning regarding incorrect type ( different signedness )
ov7670 : clean up ov7670_read semantics
mtd : mtdchar : add missing initializer on raw write
Staging : Android : ion - Fix for memory leak if ion device registration get failed .
ata : fix acpi_bus_get_device () return value check
HID : corsair : fix mapping of non - keyboard usages
[ PATCH ] fix oddball boolean logic in s390 netiucv
drm / radeon / kms : add pll debugging output
[ media ] drivers / media / video / davinci / vpbe . c : introduce missing kfree
bnx2x : Force fundamental reset for EEH recovery
[ NETFILTER ]: recent match : missing refcnt initialization
drm / i915 : intel_frontbuffer_flip_prepare () doesn ' t need struct_mutex
befs : fix leak
[ XFS ] Wait for all I / O on truncate to zero file size
ALSA : hda - Don ' t refer ELD when unplugged
include / linux / fs . h : fix userspace build
dm mpath : do not fail paths after integrity errors
[ media ] m5mols : Fix bug in stream on handler
ALSA : hdsp : silence and underflow warning
ALSA : hda - Detect non - SPDIF digital I / O
mfd : sec - core : Enable RTC on S2MPS11 PMIC
b43 : Fix swatch warning
[ PATCH ] scsi : device_reprobe () can fail
regmap : Fix ' ret ' would return an uninitialized value
ipv4 : Limit size of route cache hash table
dmaengine : i . MX : Fix merge of cookie branch .
ALSA : hda / jack - Fix the assignment of input jack - type
cifs : integer overflow in parse_dacl ()
jfs : fix d_revalidate oopsen on NFS exports
sky2 : print Optima chip name
igb : Avoid uninitialized advertised variable in eee_set_cur
[ S390 ] Fix timeval regression on s390
iio : buffer : Free active scan mask in iio_disable_all_buffers ()
NVMe : End sync requests immediately on failure
staging : comedi : disallow COMEDI_DEVCONFIG on non - board minors
uprobes : Allocate -> utask before handler_chain () for tracing handlers
hwmon : ( emc1403 ) Remove unnecessary hwmon_device_unregister
cpufreq : pcc : call CPUFREQ_POSTCHANGE notfier in error cases
ALSA : pcm : don ' t override timestamp unconditionally
spidernet : fix memory leak in spider_net_stop
fuse : fix retrieve length
xfs : prevent leaking uninitialized stack memory in FSGEOMETRY_V1
perf hists browser : Fix segfault if use symbol filter in cmdline
iwlwifi : mvm : disable uAPSD due to bugs in the firmware
[ ARM ] fix leak in iop13xx / pci
[ PATCH ] Char : tty , delete wake_up_interruptible after tty_wakeup
[ media ] rtl820t : Add a debug msg when PLL gets locked
[ XFS ] Fix hang after disallowed rename across directory quota domains
kernel / sys . c : fix missing rcu protection for sys_getpriority ()
drm / i915 : mark a newly - created GEM object dirty when filled with data
drm / ttm : Don ' t clear page metadata of imported sg pages
f2fs : report - ENOENT for unreached data indices
amd - xgbe - phy : Fix build break for missing declaration
brcmfmac : initialize escan function pointer during scheduled scan
f2fs : avoid unneeded lookup when xattr name length is too long
ocfs2 : copy fs uuid to superblock
[ BOND ]: Fix warning in bond_sysfs . c
mmc : omap_hsmmc : Update error code for response_busy cmd
iwlagn : block regular host commands if driver don ' t own uCode
ext4 : truncate_inode_pages () in orphan cleanup path
sfc : Use kzalloc () to ensure struct efx_spi_device is fully initialised
drm / radeon / kms : set fbdev_info for suspend / resume
usb : ehci - mxc : check for pdata before dereferencing
usb : dwc3 : ep0 : move to CONFIGURED also on delayed status
[ PATCH ] IB / mthca : allocate correct number of doorbell pages
cfq - iosched : Respect ioprio_class when preempting
drm / i915 : fixup assert_pipe to take the pipe A quirk into account
USB : fix interface sysfs file - creation bug
mm : check that we have the right vma in __access_remote_vm ()
Input : pmic8xxx - pwrkey - pass correct pointer to free_irq ()
gpio - mcp23s08 : correctly handling failed allocation
xtensa : fix sysmem reservation at the end of existing block
writeback : laptop_mode_timer_fn () needs rcu_read_lock () around bdi_writeback iteration
hfsplus : use - ENOMEM when kzalloc () fails
platform - drivers - x86 : intel_menlow : add missing return AE_OK for intel_menlow_register_sensor ()
[ PATCH ] zd1211rw : Add ID for Sitecom WL - 162
leds : Fix trigger unregister_simple if register_simple fails
x86 : BUILD_IRQ say . text to avoid . data . percpu
ASoC : 88pm860x - codec : Cleanup manual bias level transitions
[ PATCH ] compat_sys_nfsservctl (): handle errors correctly
tpm_tis : free irq after probing
perf tools : Try to lookup kernel module map before creating one
Staging : unisys : Use kcalloc instead of kzalloc to allocate array
mac80211 : fix memory leak
drm / nouveau : No need to force evict = true when swapping evicted BOs back in .
ext4 : add missing kfree () on error return path in add_new_gdb ()
rtc - at91rm9200 : add match - table compile guard
staging : hfi1 : sdma : Use setup_timer
[ SCSI ] ibmvfc : Fix invalid error response handling
mwl8k : Disable tx_wait completion
ALSA : asihpi - Fix outstream start trigger for non - mmap adapters .
dm raid1 : fail writes if errors are not handled and log fails
spi_mpc8xxx : Fix uninitialized variable
USB : musb : forward debug mode feature to gadget
clk : shmobile : div6 : Avoid division by zero in . round_rate ()
ceph : fix ack counter reset on connection reset
fs_enet : Fix a memory leak in fs_enet_mdio_probe
cpufreq : powernv : Free ' chips ' on module exit
drm / i915 / sdvo : Cleanup connector on error path
hwmon : ( k10temp ) Remove duplicate pci - id define
iio : accel : bmc150 - accel : fix counting direction
perf kmem : Fix compiler warning about may be accessing uninitialized variable
[ XFS ] fix valid but harmless sparse warning
esas2r : fix an error path in esas2r_ioctl_handler
raid5 : disable batch with log enabled
fuse : reset waiting
dmaengine : sirf : off by one in of_dma_sirfsoc_xlate ()
clk : ti : apll : not allocating enough data
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
x86 - 64 : Don ' t use pointer to out - of - scope variable in dump_trace ()
genirq : NULL struct irq_desc ' s member ' name ' in dynamic_irq_cleanup ()
ALSA : bebob : Fix a missing to unlock mutex in error handling case
drivers : net : cpsw : irq not disabled in cpsw isr in particular sequence
drm : sti : Fix return value check in sti_drm_platform_probe ()
cgroup : fix an off - by - one bug which may trigger BUG_ON ()
audit : Handle embedded NUL in TTY input auditing
um : don ' t leak floating point state and segment registers on execve ()
[ SCSI ] bnx2i : Added reconnect fix connecting against Lefthand targets
ipmi : Clear drvdata when interface is removed
batman - adv : remove broadcast packets scheduled for purged outgoing if
pkt_sched : sch_generic : Add Qdisc_ops peek () method .
HID : debug : fix error handling in hid_debug_events_read ()
KVM : x86 emulator : VM86 segments must have DPL 3
usb : musb : gadget : clear gadget_driver when gadget is stopped
clk : versatile : free icst on error return
clk : divider : return real rate instead of divider value
drm / radeon : check kmalloc () for failures
UBI : bugfix : dont oops with NULL module parameter
svcrpc : make svc_create_xprt enqueue on clearing XPT_BUSY
usb : chipidea : udc : . pullup is valid only when vbus is there
pcmcia : rsrc_nonstatic : check value , not pointer
iommu / amd : Fix BUG when faulting a PROT_NONE VMA
x86 apic_32 . c section fix
kgdb : add missing __percpu markup in arch / x86 / kernel / kgdb . c
drm / i915 : synchronize_irq () before turning off disp2d power well on VLV / CHV
kvm : irqchip : fix memory leak
sd : Clear PS bit before Mode Select .
ASoC : ad1836 : fix a memory leak if another ad1836 is registered
Input : turbografx - clear unused function pointers
vhost - net : fix vq_memory_access_ok error checking
ide : add hwif -> chipset fixup to ide_device_add ()
gpio : max730x : eliminate double free
leds : turn the blink_timer off before starting to blink
vm : fix incorrect unlock error path in madvise_free_huge_pmd
ath9k : No need for that extra memcpy
mmc : mxcmmc : handle mmc_of_parse () errors during probe
sched : Fix incorrect sanity check
ALSA : hda - Fix NULL dereference with CONFIG_SND_DYNAMIC_MINORS = n
sysctl : Support vdso_enabled sysctl on SH .
Input : edt - ft5x06 - ignore touchdown events
staging : comedi : ni_nio_common : don ' t write non - existing caldac ' s
[ POWERPC ] Fix rtas_flash procfs interface
PCI : host : Add of_pci_get_host_bridge_resources () stub
[ POWERPC ] Fix powermac late initcall to only run on powermac
ath6kl : Fix bug in scheduling hb_timer
[ media ] sony - btf - mpx : Drop needless newline in param description
wl12xx : minor fix in sched_scan_ssid_list
perf python : Initialize ' page_size ' variable
wlcore : save last regdom configuration on stop
[ ALSA ] hda - code : Fix the array size of codec list
i40e : Add missing braces to i40e_dcb_need_reconfig ()
ALSA : hda - Skip ELD notification during PM process
KVM : PPC : Add emulation for dcba
perf trace / scripting : Check return val of perl_run ()
clocksource : of : Respect device tree node status
[ PATCH ] knfsd : nfsd4 : fix open permission checking
ext4 : don ' t save the error information if the block device is read - only
ssb : Fix error when V1 SPROM extraction is forced
cxl : Reset default context for vPHB on release
perf tools : Add missing closedir in multi tracepoint processing
virtio : fix suspend when using virtio_balloon
USB : Deref URB after usbmon is done with it
vfs : fix propagation of atomic_open create error on negative dentry
vmalloc : remove vmalloc_sync_all () from alloc_vm_area ()
KVM : destroy workqueue on kvm_create_pit () failures
pinctrl : pinctrl - imx1 - core . c : Cleaning up if unsigned is less than zero
HID : hyperv : match wait_for_completion_timeout return type
[ SCSI ] scsi_dh : Check for sdev state in store_dh_state ()
drm / radeon / kms : Fix NULL ptr dereference
staging : iio : hmc5843 : Set iio name property in sysfs
amd / powerplay : don ' t enable ucode fan control if vbios has no fan table
ipv4 : enable getsockopt () for IP_NODEFRAG
ath10k : fix error handling in pci_probe
btrfs : Fix memory leak in btrfs_read_fs_root_no_radix ()
[ SCSI ] qla2xxx : Perform firmware dump procedure on mailbox command timeout .
drm / i915 : Reset vma -> mm_list after unbinding
[ IPSEC ] Set byid for km_event in xfrm_get_policy
[ PATCH ] PCI Hotplug : shpchp driver doesn ' t program _HPP values properly
ethtool : Fix potential kernel buffer overflow in ETHTOOL_GRXCLSRLALL
caif - hsi : Add missing return in error path
rt2x00 : Don ' t initialize MM40 HT protection to RTS / CTS on PCI devices
[ media ] radio - aimslab . c needs # include < linux / delay . h >
ALSA : hda / generic - Check power state cap at updating the widget power
Btrfs : don ' t panic if orphan item already exists
Btrfs : fix scheduler warning when syncing log
scsi : storvsc : fix SRB_STATUS_ABORTED handling
powerpc : kmalloc failure ignored in vio_build_iommu_table ()
thinkpad_acpi : Fix a memory leak during module exit
staging : vt6656 : check ieee80211_bss_conf bssid not NULL
cfg80211 : off by one in nl80211_trigger_scan ()
[ PATCH ] ext3 : Wrong return value for EXT3_IOC_GROUP_ADD
staging : crystalhd : fix a style warning
mac80211 : tx , use dev_kfree_skb_any for beacon_get
devres : WARN () and return , don ' t crash on device_del () of uninitialized device
[ SCSI ] ibmvfc : Fix DMA mapping leak on memory allocation failure
fix wrong argument of tc35815_read_plat_dev_addr ()
perf : Fix the software events state check
perfcounters / powerpc : fix oops with multiple counters in a group
cfq - iosched : make seek_mean converge more quickly
[ IA64 ] set_thread_area fails in IA32 chroot
ARM : shmobile : r8a7794 : Reserve memory as other R - Car Gen2 SoCs
[ PATCH ] coverity : tty_ldisc_ref return null check
ACPICA : Fix for memory leak related to DdbHandle objects
drivers / net / hamradio / 6pack . c : move a dereference below a NULL test
[ media ] vb2 : fix compiler warning
drivers : sh : Enable PM runtime for new R - Car Gen2 SoCs
[ PATCH ] Fix sys_migrate_pages : Move all pages when invoked from root
Staging : agnx : Move a dereference below a NULL test
[ PATCH ] ppc64 : lparconfig . c memory leak
wlcore : initialize per - link FW freed blocks correctly
staging : comedi : ni_at_ao : fix namespace clash of ' RSI ' define
mm : cma : fix condition check when setting global cma area
hso : Add new product ID
ALSA : als4000 : enable burst mode
tcp : Fix tcp_mark_head_lost () with packets == 0
ALSA : hda - Update automute / automic upon jack retasking
Staging : wlan - ng : memsetting the wrong amount of data
iommu / vt - d : Fix memory leak in dmar_insert_one_dev_info ()
drm / i915 : fix dmabuf vmap support
drm : fixes when i2c encoder slave mode_fixup is null .
[ XFS ] Fix lock inversion in forced shutdown .
[ media ] uvcvideo : Constify the uvc_entity_match_guid arguments
cpumask : Use nr_cpu_ids in seq_cpumask
[ ARM ] 4557 / 1 : Fix PXA irq gpio initialization
[ ALSA ] ASoC - Bit clock matching error
sh : force dcache flush if dcache_dirty bit set .
[ media ] w9966 : zero device state after a detach
eeepc - laptop : store_cpufv : return error if set_acpi fails
KVM : nVMX : Report 2MB EPT pages as supported
USB : Fix option_ms regression in 2 . 6 . 31 - rc2
usb : dwc3 : gadget : ignore endpoint IRQs when endpoint is disabled
e1000e : magic number cleanup - ETH_ALEN
bridge : Clear IPCB before possible entry into IP stack
staging : comedi : das16m1 : use comedi_buf_write_samples ()
perf thread : Allow deletion of a thread with no map groups
[ media ] tda10071 : fix returned symbol rate calculation
mm : alloc_large_system_hash check order
sctp : disable max_burst when the max_burst is 0
[ PATCH ] hrtimer : validate timespec of do_sys_settimeofday
ext4 : don ' t leave i_crtime . tv_sec uninitialized
ALSA : pcm : fix delta calculation at boundary wraparound
perf tools : Set vmlinux_path__nr_entries to 0 in vmlinux_path__exit
staging : gdm72xx : underflow in netlink_rcv_cb ()
staging / slicoss : disable pci device at remove
net / 9p : Initialize opts -> privport as it should be .
[ PATCH ] hrtimer : call get_softirq_time () only when necessary in run_hrtimer_queue ()
be2net : take care of __vlan_put_tag return value
mac80211 : fix ie memory allocation for scheduled scans
mt7601u : don ' t warn about devices without per - rate power table
drm / i915 / skl : Add missing SKL ids
staging : iio : Documentation : rewrite the right hand side of an assignment
staging : drm / omap : allocation fail check
[ PATCH ] spufs fix spu_acquire_runnable error path
xfs : validate acl count
drm / nvc0 / ltcg : mask off intr 0x10
mac80211 : drop 4 - addr VLAN frames earlier if not connected
[ media ] cx25821 : do not expose broken video output streams
[ media ] dvb_frontend : fix compiler warning
drivers / scsi / aacraid / commctrl . c : fix mem leak in aac_send_raw_srb ()
[ PATCH ] make kernel / printk . c : ignore_loglevel_setup () static
x86 : correct the CPUID pattern for MSR_IA32_MISC_ENABLE availability
ALSA : rme32 . c irq enabling after spin_lock_irq
usb : xhci : exit early in xhci_setup_device () if we ' re halted or dying
drm / r600 : fix memory leak introduced with 64k malloc avoidance fix .
fs / proc / task_mmu . c : check the return value of mpol_to_str ()
OMAPDSS : HDMI : init output earlier
USB : pl2303 : fix return value of tiocmset
[ IPV6 ]: Fix crash in ip6_del_rt
perf : Fix perf_event_init_context ()
x86 : fix runtime error in arch / x86 / kernel / cpu / mcheck / mce_amd_64 . c
ipv4 : netfilter : ip_tables : fix information leak to userland
USB : EHCI : add check for wakeup / suspend race
ath9k / htc_drv_main : off by one error
i386 : fix NMI watchdog not reserving its MSRs
Btrfs : fix possible memory leak in btrfs_create_tree ()
crypto : cts - Weed out non - CBC algorithms
fuse : Initialize total_len in fuse_retrieve ()
ipv6 : Fix ip6_xmit to send fragments if ipfragok is true
sunrpc : fix rpc debugging
mac80211 : Ensure initializing private mc_list in prepare_multicast ().
ALSA : fix excessive background noise introduced by OSS emulation rate shrink
sh_eth : Check for DMA mapping errors on transmit
sound : snd_ctl_elem_add : fix value count check
drm / i915 : check for supported depth at fb init time
mm : numa : Do not account for a hinting fault if we raced
zd1211rw : use unaligned safe memcmp () in - place of compare_ether_addr ()
mtd : lart : initialize writebufsize
drm / radeon : unlock the ring mutex while waiting for the next fence
ixgbe : Deny MACVLAN requests from VFs with admin set MAC
x86 / entry : Fix _TIF_USER_RETURN_NOTIFY check in prepare_exit_to_usermode
ALSA : hda - Fix digital mic on dell - m4 - 1 and dell - m4 - 3
USB : atmel_usb_udc : Use kzalloc () to allocate ep structures
[ media ] media : am437x - vpfe : return error in case memory allocation failure
ASoC : Check for NULL register bank in snd_soc_get_cache_val ()
btrfs : fix null pointer dereference in btrfs_show_devname when name is null
regmap : Don ' t try to map non - existant IRQs
drbd : Fix module refcount leak in drbd_accept ()
ipv6 : Assume the loopback address in link - local scope .
bpf : avoid copying junk bytes in bpf_get_current_comm ()
Input : amijoy - add missing platform check
[ SCSI ] remove unnecessary scsi_delete_timer () call in scsi_reset_provider ()
staging : lustre : lustre : obdclass : obd_mount . c : Fix NULL dereference
powerpc / pseries : Fix possible leaked device node reference
rtlwifi : rtl8192cu : Fix unitialized struct
userns : Fix posix_acl_file_xattr_userns gid conversion
ch7006 : correctly handling failed allocation
staging : rtl8188eu : Use ODM_REG_IGI_A_11N and ODM_BIT_IGI_11N in odm_DIGInit ()
xfs : fix memory leak in xlog_recover_add_to_trans
i2c : i2c - pnx : Limit minimum jiffie timeout to 2
USB : musb_gadget_ep0 : fix unhandled endpoint 0 IRQs , again
Input : usbtouchscreen - add support for LG Flatron T1710B
mfd : Fix palmas regulator pdata missing
[ NETFILTER ]: nf_conntrack_sip : clear address in parse_addr ()
mac80211 : initialize sta -> last_rx in sta_info_alloc
ocfs2 : fix warning in ocfs2_file_aio_write ()
amd - xgbe : Check for complete packet on skb allocation error
[ media ] r820t : Fix IF scale
PCI : Fix oops in pci_vpd_truncate
ARM : imx : initialize clk_init_data . flags for clk - fixup - mux
NFSv4 : Return delegations synchronously in evict_inode
uas : Clear cmdinfo on command queue - ing
[ POWERPC ] allocation fix in ppc / platforms / 4xx / luan . c
[ media ] usbtv : correctly handling failed allocation
block : move timeout field in struct request to pack better
drm / nouveau : fix memory leak
drm / ast : Use lockless gem BO free callback
[ PATCH ] Enable RAID autorun on Mac partition tables .
[ SCSI ] qla2xxx : Ensure RISC - interrupt - enabled consistency for IS_NOPOLLING_TYPE () ISPs .
powerpc / pseries : Fix of_node_put () underflow during DLPAR remove
tty / serial : at91 : fix race condition in atmel_serial_remove
greybus : power_supply : do not release failed supply alloc
bnx2x : properly handle CFC DEL in cnic flow
ocfs2 : fix check for locally granted state during dlmunlock ()
Input : ati_remote - add missing input_sync ()
gen_nand : Test if nr_chips field is valid
isp1760 - hcd : use USB_DT_HUB
[ PATCH ] Clear task_struct -> fs_excl on fork ()
proc / sysctl : fix the int overflow for jiffies conversion
mm : highmem : export kmap_to_page for modules
staging : rtl8192e : don ' t just print an error and continue
dm : bump the ioctl version to 4 . 32 . 0
fbcon : Fix bit_putcs () call to kmalloc ( s , GFP_KERNEL )
crypto : qat - Prevent dma mapping zero length assoc data
drm / i915 : Restore lost DPLL register write on gen2 - 4
bonding : use l4 hash if available
drm / msm : fix deadlock in bo create fail path
i2c / i2c - omap : add a const qualifier
usb : musb : core : fix IRQ check
[ media ] cx88 - dsp : Use kmalloc_array () in read_rds_samples ()
jffs2 : Add missing capability check for listing trusted xattrs
drm / mga200g : Use lockless gem BO free callback
Input : synaptics - do not retrieve the board id on old firmwares
kconfig / nconf : prevent segfault on empty menu
ext4 : add check for inodes_count overflow in new resize ioctl
KVM : ppc : deliver INTERRUPT_FP_UNAVAIL to the guest
pch_uart : add spin_lock_init
[ SCSI ] Fix block queue and elevator memory leak in scsi_alloc_sdev
btrfs : Fix the bug that fs_info -> pending_changes is never cleared .
UDF : Fix a null pointer dereference in udf_sb_free_partitions
stm class : Prevent user - controllable allocations
Btrfs : set truncate block rsv ' s size
hfsplus : fix Buffer overflow with a corrupted image
xfs : Fix possible use - after - free with AIO
[ LLC ]: Restrict LLC sockets to root
i2c / scx200_acb : Don ' t use 0 as NULL pointer
ALSA : line6 : fix ifnullfree . cocci warnings
[ SCSI ] qla2xxx : Pad IOCB structure for size requirements .
tick / nohz : Fix possible missing clock reprog after tick soft restart
proc : fix the potential use - after - free in first_tid ()
drm / i915 : Correctly reject invalid flags for wait_ioctl
bridge : vlan : fix usage of vlan 0 and 4095 again
HID : wacom : fix proximity tool release
mmc : dw_mmc : fix the wrong condition checking
cpufreq : fix obvious condition statement error
[ media ] coda : add compressed flag to format enumeration output
userns : Check euid no fsuid when establishing an unprivileged uid mapping
KVM : fail KVM_SET_VCPU_EVENTS with invalid exception number
tcp : fastopen : fix rcv_wup initialization for TFO server on SYN / data
HID : extend mask for BUTTON usage page
ext4 : online defrag is not supported for journaled files
Btrfs : return - 1 when lzo compression makes data bigger
tcmu : return on first Opt parse failure
[ ETH ]: Validate address in eth_mac_addr
acer - wmi : Add support for Aspire 1830 wlan hotkey
soc : ti : qmss : fix the case when ! SMP
Btrfs : fix em leak in find_first_block_group
ASoC : Intel : Skylake : Move up pipe mem free
ARM : 6135 / 1 : mx21 / devices : fix USBOTG resource
igb : limit EEPROM access
virtio / 9p : verify device has config space
mm : numa : migrate : Set last_nid on newly allocated page
ipv4 : Fix fib_trie . c build , missing linux / vmalloc . h include .
ipv4 : Use FIELD_SIZEOF () in inet_init ().
Input : ads7846 - remove redundant regulator_disable call
USB : gadgetfs : Fix a potential memory leak in ' dev_config ()'
tracing / function - graph - tracer : fix merge
TPM : tpm_nsc : Fix a double free of pdev in cleanup_nsc
cifs : Use BUILD_BUG_ON to validate uids and gids are the same size
leds : tca6507 : silence an uninitialized variable warning
gma500 : reversed test in mdfld_dbi_dsr_exit ()
x86 , kaslr : Use char array to gain sizeof sanity
eCryptfs : Flush dirty pages in setattr
mmc : unbreak sdhci - esdhc - imx on i . MX25
drm / i915 : try not to lose backlight CBLV precision
rt2x00 : rt2800lib : fix default TX power check for RT55xx
ALSA : hdspm - Constrain periods to 2 on older cards
floppy : Add an extra bound check on ioctl arguments
ehci - hcd : Fix typo in an error message
KVM : arm64 : vgic - its : Fix vgic_its_check_device_id BE handling
spi : fsl : Use kmalloc_array () in of_fsl_spi_get_chipselects ()
NFC : Clear pn533 target structure
HID : cp2112 : fix gpio - callback error handling
pinctrl : uniphier : fix . pin_dbg_show () callback
mmc : mmci : Cascade EPROBE_DEFER from regulators .
tools lib traceevent : Handle % z in bprint format
ath10k : enable Adaptive Noise Immunity ( ANI ) by default
[ media ] gspca / stv06xx_pb0100 : Don ' t potentially deref NULL in pb0100_start ()
ACPICA : debugger : fix memory leak on Pathname
ASoC : ml26124 : Fix negative array index read
drm / radeon : use fixed PPL ref divider if needed
cfg80211 : don ' t set privacy w / o key
mac80211 : fix TX aggregation TID struct leak
Btrfs : avoid memory leak of extent state in error handling routine
x86 : trim ACPI sleep stack buffer
[ media ] mceusb : plug memory leak on data transmit
xen / gntalloc : release grant references on page free
drm / i915 : fix integer overflow in i915_gem_execbuffer2 ()
ARM : S3C6410 : update clk -> parent when setting clock source
drm / i915 : Only check for valid PP_ { ON , OFF } _DELAYS on pre ILK hardware
acm : Return ENODEV instead of EINVAL when trying to open ACM device .
b43 : stop format string leaking into error msgs
clk : ti : Check kzalloc () for failures
i2c : i2c - designware - platdrv : Drop class based scanning to improve bootup time
[ ARM ] 5315 / 1 : Fix section mismatch warning ( sa1111 )
bsg : fix bsg_poll () to return POLLOUT properly
drm / amdgpu / gfx9 : bypass clockgating setting
ARM : S3C64XX : Staticise s3c6400_sysclass
sky2 : Shut off interrupts before NAPI
ath9k : Remove pointless ath9k_ps_restore () in ath_detach ()
[ media ] radio - bcm2048 : fix missing unlock on error in bcm2048_rds_fifo_receive ()
drm / radeon / si : add dpm quirk for Oland
usb : renesas_usbhs : fixup : avoid NULL access on error case pipe detach
sfc : mark state UNINIT after unregister
Input : gpio_keys - fix a memory leak
mac80211 : allow channel change while mesh is down
drm / i915 : don ' t reallocate the compressed FB at every frame
[ XFRM ]: xfrm_migrate () needs exporting to modules .
lib / mpi / mpicoder . c : looping issue , need stop when equal to zero , found by ' EXTRA_FLAGS =- W '.
ASoC : Serialize access to dapm_power_widgets ()
knfsd : Fixed problem with NFS exporting directories which are mounted on .
[ PATCH ] null dereference in fs / jbd2 / journal . c
[ CIFS ] relinquish fscache cookie before freeing CIFSTconInfo
ALSA : es1688 : Use strcpy () instead of sprintf ()
KVM : s390 : fix signal handling
[ TG3 ]: Update version to 3 . 82 .
rose : fix info leak via msg_name in rose_recvmsg ()
perf symbols : Do not open device files
Staging : rtl8188eu / core : Coding style fix , set constant operand on right in tests
ARM : pxa : magician : Fix wrongly enabled USB host ports
dm : sysfs skip output when device is being destroyed
extcon : Fix return value in extcon - class . c
seccomp : cap SECCOMP_RET_ERRNO data to MAX_ERRNO
PCI : Prevent out of bounds access in numa_node override
gpio / samsung : only register available gpio banks
[ media ] radio - maxiradio : fix memory leak when device is removed
ALSA : timer : fix division by zero after SNDRV_TIMER_IOCTL_CONTINUE
cw1200 : Fix cw1200_debug_link_id
bonding : in balance - rr mode , set curr_active_slave only if it is up
clk : rockchip : rename rga clock - id on rk3399
drm / sti : sti_vtg : Handle return NULL error from devm_ioremap_nocache
[ PATCH ] hwrng : fix intel probe error unwind
ipv6 : ignore null_entry in inet6_rtm_getroute () too
drm / i915 : Avoid NULL ctx -> obj dereference in debugfs / i915_context_info
Staging : rtl8821ae : Drop memory allocation cast for kmalloc in hal_btc . c
tipc : Add in missing lock during link initialization
bnx2x : correct advertisement of pause capabilities
[ SPARC64 ] sunhv : Support SYSRQ properly .
pm2fb : free cmap memory on module remove
x86 , ioapic : Throw BUG instead of NULL dereference
powerpc / pmac : Fix SMP kernels on pre - core99 UP machines
xfs : fix might_sleep () warning when initialising per - ag tree
[ media ] saa7134 : v4l2 - compliance : clear reserved part of VBI structure
sched / headers : Remove the runqueue_is_locked () prototype
HID : cp2112 : fix to force single data - report reply
Staging : rtl8192e : Use ! x instead of x == NULL
ALSA : hda - fix read before array start
staging : r8188eu : Add files for new drive : Cocci spatch " noderef "
ocfs2 : o2hb : fix double free bug
x86 - 32 : Fix sparse warning for the __PHYSICAL_MASK calculation
hfsplus : handle match_strdup failure
[ SCSI ] ibmvscsi : Correct DMA mapping leak
sata - fsl : Apply link speed limits
HID : multitouch : force retrieving of Win8 signature blob
mfd : Use devm_ * APIs for mc13xxx : remove leftover kfree
[ media ] V4L : mt9t112 : use after free in mt9t112_probe ()
ath5k : initialize default noise floor
kernfs : update kernfs_rename_ns () to consider KERNFS_STATIC_NAME
mtd : mtk - nor : adjust sequence of trigger function and assignment function
hvc_xen : NULL dereference on allocation failure
xen / privcmd : print SIGBUS faults
ipvs : info leak in __ip_vs_get_dest_entries ()
Input : serio_raw - link serio_raw misc device to corresponding
ixgbe : add missing rtnl_lock in PM resume path
ALSA : usb : caiaq : check for cdev -> n_streams > 1
drm / radeon : force MSIs on RS690 asics
spi : add spi_message_init_no_memset to avoid zeroing the spi_message
xfrm_user : verify policy direction at XFRM_MSG_POLEXPIRE handler
ALSA : usb - audio : Fix quirks code is not called
gpio : GPIO_GET_CHIPINFO_IOCTL : Fix information leak
mmc : mmci : fix mmci_post_request
amd64_edac : Fix potential memleak
pinctrl : single : fix error return code in pcs_parse_one_pinctrl_entry ()
powerpc / pci : fix PCI - e check link issue
[ media ] dvb_usb_v2 : fix error handling for . tuner_attach ()
ASoC : Intel : add support for Realtek 5651 on Cherrytrail
mac80211 : fix potentially redundant skb data copying
bcma : support chipsets with PMU and GCI cores ( devices )
usb : dwc3 : gadget : stop touching HWO TRBs
KVM : x86 : race between KVM_SET_GSI_ROUTING and KVM_CREATE_IRQCHIP
ARM : OMAP : board - omap4panda : MUX configuration for sys_nirq2
staging : lustre : Replace max () with max_t ()
ARM : S3C64XX : DMA : Free node for non - circular queues
OMAP4 : Fix for CONTROL register Base
mac80211 : compete scan to cfg80211 if deferred scan fail to start
metag : export clear_page and copy_page
usb : isp1760 : check for null return from kzalloc
libceph : avoid NULL kref_put from NULL alloc_msg return
usb : gadget : dummy_hcd : clear usb_gadget region before registration
[ PATCH ] ieee1394 : fix cross_bound check for null ISO packets
viafb : fix IGA1 modesetting on VX900
pcmcia : fix incorrect bracketing on a test
[ libata ] ata_piix : add missing kfree ()
fs : Fix theoretical division by 0 in super_cache_scan ().
crypto : md5 - Set statesize
i2c - cpm : drop NO_IRQ
staging : android : ion : ion_dummy_driver . c Replace kzalloc () by kcalloc ()
[ SPARC64 ]: Fix pfn_pte () build failure .
xenbus : avoid zero returns from read ()
pppoe : Lacks DST MAC address check
dmaengine : pl330 : Check if the DMA descriptor is NULL
um : don ' t leak floating point state and segment registers on execve ()
iwlwifi : mvm : don ' t clear tbl -> win mistakenly
sfc : Clear I2C adapter structure in falcon_remove_nic ()
ALSA : hdspm - Provide ioctl_compat
[ media ] davinci : vpif_display : fix return type check for v4l2_subdev_call ()
drm / nouveau / bios / mxm : handle digital connector table 1 . 1
ipc / shm . c : check for ulong overflows in shmat
KVM : x86 : preserve x2APIC LDR on INIT
drivers / rtc / rtc - da9055 . c : remove use of regmap_irq_get_virq ()
drm / nouveau / pci / gp104 : initial support
ceph : fscache : Update object store limit after file writing
staging : dgrp : fix potential NULL defereference issue
cpu / hotplug : Handle early registration gracefully
reparent_leader : check EXIT_DEAD instead of task_detached ()
USB : serial : fix potential heap buffer overflow
futex : Prevent attaching to kernel threads
USB : Checking the wrong variable in usb_disable_lpm ()
ipv4 : Fix memory leak in exception case for splitting tries
drm / tilcdc : Remove unnecessary braces
x86 : Use string instruction memcpy on AMD Fam11h
cpufreq : powernow - k6 : Fix incorrect comparison with max_multipler
perf / x86 : Check perf_callchain_store () error
dm mpath : fix NULL pointer dereference when path parameters missing
staging : line6 : Fix unlocked snd_pcm_stop () call
ASoC : fsl_ssi : Fix samples being dropped at Playback startup
[ media ] drxk_hard : fix bad alignments
USB : xhci : drop spinlock in xhci_urb_enqueue () error path .
crypto : qat - update init_esram for C3xxx dev type
ARM : mx3 : mx31lilly : pass mc13783 SPI IRQ pin
pinctrl : tegra - xusb : fix an off by one test
hwmon : ( w83627ehf ) Force initial bank selection
Btrfs : don ' t wait around for new log writers on an SSD
x86 , efi : Check table header length in efi_bgrt_init ()
iommu / vt - d : Remove to_pci_dev () in intel_map_page ()
drm / i915 : Shut down PCH interrupts during irq_uninstall
staging : comedi : adv_pci1710 : update the MODULE_DESCRIPTION
drm : Avoid oops in GEM execbuffers with bad arguments .
Btrfs : fix a scrub regression in case of write errors
isdn : mISDN : set -> family in -> getname ()
drivers / rtc / rtc - rv3029c2 . c : fix disabling AIE irq
Drivers : hv : balloon : Make the balloon driver not unloadable
PCI : Do not treat EPROBE_DEFER as device attach failure
mac80211 : extend fast - xmit to driver fragmentation
Input : ads7846 - avoid pen up / down when reading hwmon
drm / nouveau : properly handle allocation failure in nouveau_sgdma_populate
irqdomain : Export irq_domain_free_irqs_common
[ SCSI ] lpfc : fix oops when parsing dodgy VPD
greybus : timesync / pm : Make synchronous call to restore FrameTime
imx - drm : ipuv3 - plane : fix ipu_plane_dpms ()
mei : abort waiting for notification on unsupported HW
x86 , mce : fix ifdef for 64bit thermal apic vector clear on shutdown
[ media ] r820t : better report signal strength
[ GFS2 ] kernel panic mounting volume
rtlwifi : rtl8192se : Fix gcc 4 . 7 . x warning
[ SCSI ] libfc : handle RRQ exch timeout
UIO : uio_pdrv : fix memory leak
drm / amdgpu : no cg for soc15 of SRIOV
scsi : ibmvscsis : Properly deregister target sessions
ASoC : DaVinci : pcm , constrain buffer size to multiple of period
ocfs2 : goto out_unlock if ocfs2_get_clusters_nocache () failed in ocfs2_fiemap ()
block : remove padding from io_context on 64bit builds
udf : Fix possible corruption when close races with write
hv_netvsc : fix a race between netvsc_send () and netvsc_init_buf ()
usb : gadget : r8a66597 - udc : fix error return code in r8a66597_probe ()
spi : qup : Fix return value checking for pm_runtime_get_sync ()
ALSA : hda_intel : Add the Linux device ID for NVIDIA HDA controller
fuse : fix bad return value in fuse_file_poll ()
[ media ] cx22700 : Fix potential buffer overflow
cfq : choose a new next_req when a request is dispatched
ceph : properly set issue_seq for cap release
dmaengine : st_fdma : Fix the error return code in st_fdma_probe ()
[ media ] DaVinci - VPFE - Capture : Replace a memcpy () call by an assignment in vpfe_enum_input ()
ACPICA : Fix memory leak in table load error path
UBIFS : check return code
spi : pxa2xx : missing break in pxa2xx_ssp_get_clk_div ()
net : make ipv6 PKTINFO honour freebind
[ PATCH ] Fix listxattr () for generic security attributes
[ NETFILTER ]: nfnetlink_log : fix possible NULL pointer dereference
ieee802154 : Fix oops during ieee802154_sock_ioctl
intel - iommu : fix off - by - one in pagetable freeing
hwmon : ( max31790 ) potential ERR_PTR dereference
staging : logger : hold mutex while removing reader
lib / genalloc . c : check result of devres_alloc ()
drm / i915 / skl : Restore the DDI translation tables when enabling PW1
ARM : mx51 : Fix the address space length for SSI
drm / amdgpu : update pd while updating vm as well
iommu : smar : Fix return value check of create_irq ()
Drivers : hv : vmbus : Fix a bug in vmbus_open ()
drm / i915 : WARN if module opt sanitization goes out of order
[ 9P ]: Fix missing unlock before return in p9_mux_poll_start
perf top : Add top . children config option
ixgbe : balance free_irq calls with request_irq calls
usb : xhci : plat : Enable async suspend / resume
blk - mq : stop hardware queue in blk_mq_delay_queue ()
[ media ] cxd2820r : wait demod lock for DVB - C too
virtio : console : Make read () return - ENODEV on hot - unplug
mac80211 : simplify AP interface stop
[ libata ] sata_sil : fix used - uninit warning
NFSD : Put export if prepare_creds () fail
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
proc : mem_release () should check mm != NULL
dma : pl330 : Set residue_granularity
Drivers : hv : Fix a memory leak
inet : tcp : fix inetpeer_set_addr_v4 ()
firewire : ohci : correct signedness of a local variable
drm / rockchip : return ERR_PTR instead of NULL
drivers / net / usb / usbnet . c : Use FIELD_SIZEOF macro in usbnet_init () function .
drm / i915 : take mode config lock around crtc disable at suspend
HID : i2c - hid : also call i2c_hid_free_buffers in i2c_hid_remove
staging : lustre : lustre : mdc : lproc_mdc . c : Fix for potential NULL pointer dereference
fm10k : don ' t try to stop queues if we ' ve lost hw_addr
drivers / platform / x86 / amilo - rfkill . c :: amilo_rfkill_probe () avoid NULL deref
mmc : core : fix error path in mmc_host_alloc
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
USB : cdc - acm . c : Fix compile warnings
[ media ] v4l : of : Drop acquired reference to node when getting next endpoint
tmiofb : missing NULL pointer checks
drm : mali - dp : Set the drm -> irq_enabled flag to match driver ' s state .
[ PATCH ] x86_64 : Fix swiotlb dma_alloc_coherent fallback
slub : When allocating a new slab also prep the first object
eCryptfs : protect crypt_stat -> flags in ecryptfs_open ()
i915 : fix lock imbalance on error path ...
perf record : No build id option fails
Staging : Fix leak in drivers / staging / at76_usb . c
Btrfs : check if previous transaction aborted to avoid fs corruption
OMAPDSS : HDMI5 : disable interlace modes
drm / i915 / vlv : disable PPGTT on early revs v3
xtensa : fix potential NULL - pointer dereference
[ PATCH ] pcmcia : permit single - character identifiers
USB : Add device quirk for Microsoft VX700 webcam
clk : si5351 : Fix clkout rate computation .
dma : imx - sdma : Assign a default script number for ROM firmware cases
cfq - iosched : handle failure of cfq group allocation
[ PATCH ] x86 - 64 : unexport cpu_llc_id
arm64 : fix NULL dereference in have_cpu_die ()
drm / exynos : exynos_hdmi : Pass correct pointer to free_irq ()
staging : sm7xx : Fixup of the section mismatch
crypto : eseqiv - Fix IV generation for sync algorithms
drm / i915 : Enable GMBUS for post - gen2 chipsets
drm / i915 : take struct mutex around fb unref
ceph : drop extra open file reference in ceph_atomic_open ()
mfd : Check for twl4030 - madc NULL pointer
ixgbe : use kzalloc for allocating one thing
latencytop : fix kernel panic while reading latency proc file
skge : Adjust a null pointer check in skge_down ()
video : vt8500lcdfb : Remove kfree call since devm_kzalloc () is used
vfio / spapr_tce : Check kzalloc () return when preregistering memory
[ ALSA ] emu10k1 - Fix STAC9758 front channel
i2c / ds1374 : Check workqueue creation status
Btrfs : align offsets for btrfs_ordered_update_i_size
Btrfs : Lower the max inline size to 8k
fbcon : delay the start of the cursor timer until a con_switch ()
perf symbols : Fix missing GNU IFUNC symbols
rrunner . c : fix possible memory leak in rr_init_one ()
staging : greybus : make cport_quiesce () method optional
gpio : davinci : Fix possible NULL pointer deference
jbd2 : Fix unreclaimed pages after truncate in data = journal mode
isdn : avoid copying too long drvid
bcache : fix use - after - free in btree_gc_coalesce ()
[ ALSA ] sound / isa / ad1816a / ad1816a . c : check kmalloc () return value
w1 - call request_module with w1 master mutex unlocked
axp288_fuel_gauge : Add original author details
[ SCSI ] aha152x : Fix oops on module removal
[ NETFILTER ]: H . 323 helper : fix possible NULL - ptr dereference
ASoC : si476x : Fix IO setup
ARM : 7857 / 1 : dma : imx - sdma : setup dma mask
iommu / ipmmu - vmsa : Restrict IOMMU Domain Geometry to 32 - bit address space
staging : comedi : adv_pci_dio : fix bug in ' detach '
proc : Point / proc / net at / proc / thread - self / net instead of / proc / self / net
[ SCSI ] lpfc : introduce missing kfree
drbd : Null pointer deref fix to the large " multi bio rewrite "
staging : fix go7007 - usb license
amd64_edac : simple fix to allow reporting of CECC errors
PCI : use pci_is_root_bus () in pci_read_bridge_bases ()
gpio : remember to finally free gpio_device
slab : Use proper formatting specs for unsigned size_t
staging : dgap : fix checkpatch warning on sscanf usage
sctp : fix auth_hmacs field ' s length of struct sctp_cookie
mtd : spi - nor : improve macronix_quad_enable ()
scsi : handle command allocation failure in scsi_reset_provider
extcon : trivial : kfree missed from remove path
OMAPDSS : hdmi5 : Fix bit field for IEC958_AES2_CON_SOURCE
cfg80211 : minimal error handling for wext - compat freq scanning
tipc : check return value of nlmsg_new
crypto : sahara - fix typo " Decidated " -> " Dedicated "
USB : sierra : fix urb and memory leak in resume error path
Input : polldev - immediately poll device upon opening
r8152 : replace strncpy with strlcpy
btrfs : rw_devices shouldn ' t be incremented for seed fs in btrfs_rm_dev_replace_srcdev ()
[ media ] v4l2 - ctrls . c : add NULL check
perf evlist : Fix max mmap_pages
sh_eth : enable wake - on - lan for sh7734
x86 : watchdog - check for CPU is being supported
int340x_thermal / processor_thermal_device : return failure when
cpufreq : Return error if -> get () failed in cpufreq_update_policy ()
jfs : avoid undefined behavior from left - shifting by 32 bits
ceph : clear migrate seq when MDS restarts
xenbus : remove transaction holder from list before freeing
gpio : mcp23s08 : request a shared interrupt
ALSA : hda - Sync EAPD with vmaster on AD1984A Thinkpads
ACPI : thinkpad - acpi : do not arm fan watchdog if it would not work
iio : ak8975 : Add device name
bzip2 / lzma : fix decompress_inflate . c vs multi - block - with - embedded - filename
ext4 : error proc entry creation when the fs / ext4 is not correctly created
spi : spi - ti - qspi : reinit of completion variable
xfs : fix assertion failure at xfs_setattr_nonsize
usb : gadget : f_printer : actually limit the number of instances
Staging : ozwpan : prevent bogus dereference
spi : core : Fix check for dma_map_sg () failures
batman - adv : Count all non - success TX packets as dropped
blkio : Set must_dispatch only if we decided to not dispatch the request
xfs : fix duplicate message output
b43 : fix support for 14e4 : 4321 PCI dev with BCM4321 chipset
[ POWERPC ] spufs : check spu_acquire_runnable () return value
apm_power : check I . intval for zero value , we use it as the divisor
Staging : comedi : dt2801 . c : off by one issue
pinctrl : mediatek : add input - enable setting in gpio_request_enable
V4L : Storage class should be before const qualifier
ixgbe : remove ifdef check for non - existent define
[ GFS2 ] Fix memory allocation in glock . c
sched : Try to deal with low capacity , fix update_sd_power_savings_stats ()
x86 : Update UV3 hub revision ID
Input : gamecon - fix off by one range check
staging / rts_pstor : remove braces {} in sd . c ( sd_sdr_tuning )
[ MIPS ] time : set clock before clockevent_delta2ns () in GT641xx .
drm / i915 : Don ' t call sg_free_table () if sg_alloc_table () fails
drm : fix division - by - zero on dumb_create ()
igb : cap interrupts at 20K per queue when in itr mode 3
[ media ] media : rcar_vin : Reject videobufs that are too small for current format
be2net : fix pmac_id [] allocation size
qlcnic : Fix ordering of stats in stats buffer .
fs / exofs / ore_raid . c : replace count * size kzalloc by kcalloc
staging : comedi : pcl726 : update MODULE_DESCRIPTION
drm / tegra : Use lockless gem BO free callback
staging : ft1000 : fix kernel info leak to userland for statistics
Btrfs : remove dead code
mmc : sdhci - esdhc - imx : silence a false curly braces warning
ext3 : destroy sbi mutexes in put_super
[ PATCH ] device - mapper snapshot : bio_list fix
staging : vt6655 : check ieee80211_bss_conf bssid not NULL
ASoC : omap - mcbsp : fix snd_pcm_hw_rule_add arguments
eeepc - wmi : set the right key code for 0xe9
drm / nouveau : Fix detection of DDC - based LVDS on DCB15 boards .
ASoC : Make struct snd_soc_card ' s dapm_widgets and dapm_routes const
ceph : add missing locking to protect i_snap_realm_item during split
drm : Perform ioctl command validation on the stored kernel values
Input : atmel_mxt_ts - add bootloader addresses for new chips
xfs : invalidate allocbt blocks moved to the free list
ASoC : Free card DAPM context on snd_soc_instantiate_card () error path
geneve : Unregister pernet subsys on module unload .
bnx2x : Prevent " Unknown MF " print in SF mode
w1 : w1_process () is not freezable kthread
ARM : zynq : initialize slcr mapping earlier
[ NETFILTER ]: xt_pkttype : Add explicit check for IPv4
Btrfs : add fiemap ' s flag check
ceph : handle ceph_osdc_new_request failure in ceph_writepages_start
xen / balloon : Don ' t continue ballooning when BP_ECANCELED is encountered
x86 , quirk : Fix SB600 revision check
ARM : OMAP2 : Prepare cpu detection for further improvements
mac80211 : Change WARN_ON to WARN_ON_ONCE
drm / i915 : Track the primary plane correctly when reassigning planes
sched / balancing : Fix ' local -> avg_load > sds -> avg_load ' case in calculate_imbalance ()
ceph : reset iov_len when discarding cap release messages
drm / radeon / dpm : fix typo in setting uvd clock
ext4 : remove another test in ext4_alloc_file_blocks ()
[ PATCH ] mv643xx_eth : Update dev -> last_rx on packet receive
net : avoid NULL deref in inet_ctl_sock_destroy ()
igb : Bump version to 3 . 0 . 6
[ media ] ascot2e : Fix I2C message size check
kvm / s390 : also set guest pages back to stable on kexec / kdump
USB : isp1760 : Use an IS_ERR test rather than a NULL test
perf tools : Do not auto - remove Children column if -- fields given
[ media ] marvell - cam : Don ' t signal multiple frame completions in scatter / gather mode
hwmon : ( f75375s ) Disable setting DC fan control mode for F75373
usb : musb : blackfin : fix typo in new bfin_musb_vbus_status func
[ TCP ]: Clear stale pred_flags when snd_wnd changes
ocfs2 : Improve rename locking
batman - adv : remove extra negation in gw_out_of_range ()
dmaengine : zxdma : Fix off - by - one for testing valid pchan request
[ PATCH ] sata_sil24 : kill 10ms sleep in softreset
[ Bluetooth ] Revert session reference counting fix
arm / arm64 : KVM : Don ' t allow creating VCPUs after vgic_initialized
UBI : Silence an unintialized variable warning
tty : an overflow of multiplication in drivers / tty / cyclades . c
[ media ] em28xx - i2c : rt_mutex_trylock () returns zero on failure
ARM : OMAP2 +: Fix dmtimer set source clock failure
btrfs : remove unnecessary list_del
mfd : twl4030 - power : Fix hang on reboot if sleep configuration was loaded earlier
atl1c : return PCI_ERS_RESULT_DISCONNECT on permanent failure
lpfc : Fix internal loopback failure .
gianfar : Do not reuse pages from emergency reserve
drm / tilcdc : Take CRTC lock when calling tilcdc_crtc_disable ()
pktgen : fix crash with vlan and packet size less than 46
[ PATCH ] PCI : fix memory leak in MMCONFIG error path
staging : comedi : jr3_pci : check PCI BAR 0 size
Staging : ramzswap : Set block size to PAGE_SIZE
drivers / rtc / rtc - snvs . c : remove redundant of_match_ptr
Staging : slicoss : changes memory allocation style in slicoss . c
dns_resolver : assure that dns_query () result is null - terminated
drm / i915 / bxt : Use PHY0 GRC value for HW state verification
evm : properly handle INTEGRITY_NOXATTRS EVM status
PCI : hv : Use the correct buffer size in new_pcichild_device ()
clk / axs10x : Clear init field in driver probe
acer - wmi : set the touchpad toggle key code to KEY_TOUCHPAD_TOGGLE
perf symbols : Fix random fd closing with no libelf
UBI : always re - read in case of read failures
staging : iio : Make write_event_value callback optional
[ CIFS ] add extra null attr check
isdn / hisax : fix compiler warning on hisax_pci_tbl
[ media ] bt8xx : fix memory leak
logfs : maintain the ordering of meta - inode destruction
drm / amdgpu : update rev id for oland
ASoC : Check for errors when writing WM8731 reset register
mwifiex : fix corner case association failure
drivers / perf : arm_pmu : Avoid leaking pmu -> irq_affinity on error
panic , vt : do not force oops output when panic_timeout < 0
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
Staging : rtl8712 : fix potential null pointer dereference
leak in do_ubd_request
usbnet : smsc95xx : don ' t recover device if suspend fails in system sleep
thermal : imx : Fix possible NULL dereference .
[ SCSI ] isci : fix breakage caused by > 16byte CDB patch
KVM : PPC : Book3S HV : Don ' t try to signal cpu - 1
mtd : docg3 : fix BCH registers
arch / powerpc / sysdev / fsl_rmu . c : introduce missing kfree
tpm / st33zp24 / spi : Improve st33zp24_spi_evaluate_latency
ALSA : asihpi : Fix unlocked snd_pcm_stop () call
[ PATCH ] relax ide - cd dma restrictions
KVM : s390 : implement KVM_CAP_NR / MAX_VCPUS
exit : proc : don ' t try to flush / proc / tgid / task / tgid
gpio : use kzalloc to allocate gpio_device
lkdtm : do not leak free page on kmalloc failure
ACPICA : fix mutex names in debug code .
proc : task_state : read cred -> group_info outside of task_lock ()
crypto : amcc - check return value of sg_nents_for_len
Btrfs : fix incorrect space accounting after failure to insert inline extent
virtio_console : Don ' t access uninitialized data .
rapidio / tsi721 : fix incorrect detection of address translation condition
x86 / acpi : Prevent LAPIC id 0xff from being accounted
staging : et131x : stop read when hit max delay in et131x_phy_mii_read
usb : fusbh200 - hcd : fix error handling in fusbh200_hcd_fusbh200_probe ()
drm / i915 : enable HDMI mode on VLV when an HDMI sink is detected
crypto : talitos - avoid memleak in talitos_alg_alloc ()
kobject : free memory if netlink_kernel_create () fails
[ ALSA ] gusextreme : set codec_flag
xfs : silence uninitialised f . file warning .
[ SCSI ] nsp_cs : fix buf overflow
pcmcia : check for pointer instead of pointer address
staging : sep : add missing misc_deregister () on error in sep_register_driver_with_fs ()
mfd : palmas : Initialise client -> of_node for dummy created client
staging : unisys : remove S8 type
crypto : camellia_aesni_avx - Fix CPU feature checks
PCI : Use dev_is_pci () to identify PCI devices
ASoC : rt5645 : Check if codec is initialized in workqueue handler
tridentfb : fix hang on Blade3D with CONFIG_CC_OPTIMIZE_FOR_SIZE
perf tools : Change perf_guest default back to false
squashfs : add missing buffer free
ocfs2 : Re - order iput in ocfs2_drop_dentry_lock
drm / i915 : Simplify i915_gem_obj_is_pinned () test for set - tiling
[ PATCH ] v9fs : fix fd_close
[ SCSI ] mpt2sas : add missing allocation .
drm / vmwgfx : Don ' t flush fb if we ' re in the suspended state .
sh - pfc : r8a7790 : Fix definition of IPSR5
[ ARM ] pxa : add missing irq events for pxa168
sysctl : require CAP_SYS_RAWIO to set mmap_min_addr
bonding : use rcu_access_pointer () in bonding_show_mii_status ()
drm / exynos : check crtc ' s dpms mode at SetCrtc
[ media ] dvb_demux : fix deadlock in dmx_section_feed_release_filter ()
[ PATCH ] savagefb : Remove NULL check
drm / i915 : Convert wait_for ( I915_READ ( reg )) to intel_wait_for_register ()
[ PATCH ] inotify : fix idr_get_new_above usage
drm / i915 / dp : add lane_count check in intel_dp_check_link_status
tracing : Use TRACE_MAX_PRINT instead of constant
ALSA : ALC283 codec - Avoid pop noise on headphones during suspend / resume
iwlwifi : fix PS disable status race
[ PATCH ] libata : fix missing classes [] initialization in ata_bus_probe ()
ASoC : rsnd : rsnd_dai_connect () returns error if it connect to existing mod
blk - throttle : check stats_cpu before reading it from sysfs
phy : Add reset callback for not generic phy
cpufreq : brcmstb - avs - cpufreq : remove unnecessary platform_set_drvdata ()
ipv6 : Fix mem leak in rt6i_pcpu
fealnx : Write outside array bounds
mac802154 : llsec : add forgotten list_del_rcu in key removal
f2fs : fix wrong max cost initialization
svcrdma : Free context on post_recv error in send_reply
trusted - keys : check for NULL before using it
ASoC : intel : log an error on double free
selinux : check ss_initialized before revalidating an inode label
[ PATCH ] hugetlb : check p ? d_present in huge_pte_offset ()
firewire : nosy : fix IRQ handler for card ejection
staging : bcm2835 - audio : Fixed initialization of global to " false "
greybus : uart fix missing negation on DTR setting
mlx5_core : Fix out arg size in access_register command
[ PATCH ] VFS : memory leak in do_kern_mount ()
KVM : nVMX : Remove redundant get_vmcs12 from nested_vmx_exit_handled_msr
drm / radeon : Fail fb creation from imported dma - bufs .
sysctl : Drop reference added by grab_header in proc_sys_readdir
ipv6 - multicast : Fix memory leak in input path .
[ PATCH ] imsttfb : Fix resource leak
drm / nouveau / i2c / gp104 : initial support
[ PATCH ] m32r : __cmpxchg_u32 fix
ARM : SMP : remove IRQ - disabling for smp_cross_call ()
perf bench : Fix NULL pointer dereference in " perf bench all "
drm / amdgpu / si : load the proper firmware on 0x87 oland boards
igb : add i211 to i210 PHY workaround
[ SCSI ] libfcoe : don ' t send ELS in FIP mode if no FCF selected
Staging : bcm : Remove assignment in if condition reported by checkpatch . pl
i82875p_edac : fix overflow device resource setup
[ PATCH ] Uninitialized variable in drivers / net / wan / syncppp . c
e1000 : fix WARN_ON with mac - vlan
qxl : check for kmap failures
drivers : usb : Mark function as static in usbsevseg . c
[ IPV4 ]: Make tcp_input_metrics () get minimum RTO via tcp_rto_min ()
[ PATCH ] USB : get USB suspend to work again
[ media ] marvell - cam : power down mmp camera on registration failure
drm : i915 : Explicitly apply PWM config extracted from pwm_args
ceph : fix leak of mon authorizer
tmpfs : fix highmem swapoff crash regression
ASoC : Return correct codec clock in s3c64xx - i2s
ath9k : deny new interface addtion on IBSS mode
perf session : Fix crash with invalid CPU list
x86 : Re - get cfg_new in case reuse / move irq_desc
selinux : fix off - by - one in setprocattr
NFS : Return directly if encode_sessionid fail
fs / ramfs / file - nommu . c : replace count * size kzalloc by kcalloc
iio : light : ltr501 bug in parameter sanity check .
kernel / kmod . c : check for NULL in call_usermodehelper_exec ()
btrfs : reada : move reada_extent_put to place after __readahead_hook ()
spi - imx : initialize complete config struct
x86 , amd - ucode : Check UCODE_MAGIC before loading the container file
TG3 : limit reaches - 1
sdhci : make isr tolerant of read errors
mtd : blkdevs : fix crash related to sysfs attributes .
dlm : close othercons
misc : mei : set IRQF_ONESHOT for msi request_threaded_irq
iw_cxgb4 : initialize ibdev . iwcm -> ifname for port mapping
edac : mv64x60 fix get_property
i40iw : Add NULL check for ibqp event handler
x86 / mce / AMD : Make the init code more robust
usb : dwc3 : gadget : return error if command sent to DGCMD register fails
Input : rohm_bu21023 - fix handling of retrying firmware update
bpf , mips : fix off - by - one in ctx offset allocation
igb : Change memcpy to struct assignment
macvlan : make operstate and carrier more accurate
rbd : reduce memory used for rbd_dev fields
i2c : mux : reg : Provide of_match_table
[ PATCH ] Fix warning with b44 . c on 64bit boxes
p54usb : fix leaks at failure path in p54u_probe ()
drivers / w1 / masters / omap_hdq . c : fix missing mutex unlock
drm / exynos / fbdev : set smem_len for fbdev
ALSA : snd - powermac : timeout reaches - 1
af_packet : remove unnecessary BUG_ON () in tpacket_destruct_skb
ARM : EXYNOS : fix GIC using for EXYNOS5440
Drivers : hv : vss : run only on supported host versions
tracing / filters : free filter_string in destroy_preds ()
i2c : i2c - bcm - kona : Fix module build
[ ALSA ] soc - Clean up tabs
libceph : avoid NULL kref_put when osd reset races with alloc_msg
dmaengine : coh901318 : Only calculate residue if txstate exists .
ssb : Use pci_is_pcie ()
[ PATCH ] fix PROMISC / bridging in TLAN driver
vmwgfx : information leak in vmw_execbuf_copy_fence_user ()
slab numa fallback logic : Do not pass unfiltered flags to page allocator
drm / radeon / kms / DCE4 . 1 : ss is not supported on the internal pplls
[ PPPOE ]: Missing result check in __pppoe_xmit ().
[ PATCH ] x86 : initialise tss -> io_bitmap_owner to something
davinci : DM365 : Update NAND driver platform data
arch / x86 / kernel / kdebugfs . c : introduce missing kfree
lib : list_sort_test (): add extra corruption check
NetLabel : the CIPSOv4 passthrough mapping does not pass categories correctly
input : pmic8xxx - pwrkey : Do not use mfd_get_data ()
iio : staging : ad7298_ring : Fix maybe - uninitialized warning
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
e1000e : Correctly include VLAN_HLEN when changing interface MTU
[ NETFILTER ]: recent match : missing refcnt initialization
Btrfs : remove btrfs_try_spin_lock
slab : fix typo in allocation failure handling
drm / ttm : fix stupid parameter inversion in the pipeline code
Input : tegra - kbc - correct call to input_free_device
pinctrl : sunxi : Fix potential null pointer dereference
usb : early : fixed coding style issue related to : operator
bnx2x : Change to driver version 1 . 72 . 10 - 0
mm , fs : check for fatal signals in do_generic_file_read ()
iio : adc : ad799x : remove space before comma
[ TCP ]: Fix oops caused by __tcp_put_md5sig_pool ()
usb : gadget : bdc_udc : set value for common is_selfpowered
clk : divider : Fix table round up function
ocfs2 : honor O_ ( D ) SYNC flag in fallocate
capabilities : initialize has_cap
usb : udc : allow adding and removing the same gadget device
i40e : Fix memory leak at failure path in i40e_dbg_command_write ()
[ PATCH ] TPM : fix failure path leak
drm : WARN when removing unallocated node
staging : most : add missing channel initialization
ceph : skip mds sync on forced unmount
remove header_ops bug in qeth driver
IPv6 : use ipv6_addr_copy () in ip6_route_redirect ()
mac80211 : fix rate control initialisation
bnx2x : fix possible panic under memory stress
staging : comedi : disallow COMEDI_DEVCONFIG on non - board minors
firewire : fw - cdev : reorder wakeup vs . spinlock
iio : ina2xx : give the capture kthread a more useful name string .
hwmon : ( emc1403 ) Remove unnecessary hwmon_device_unregister
ath9k : Do not use local_bh_disable in ampdu_action
virtio_net : enable big packets for large MTU values
[ PATCH ] i4l : memory leak fix for sc_ioctl ().
blkcg : fix double free of new_blkg in blkcg_init_queue
pinctrl : s3c24xx : off by one in s3c24xx_eint_init ()
ALSA : pcm : don ' t override timestamp unconditionally
usbnet : allow mini - drivers to consume L2 headers
md : fix bug in raid10 recovery .
iio : ti_am335x_adc : Fix : Use same step id at FIFOs both ends
linux / kernel . h : define SYMBOL_PREFIX
[ ARM ] 5402 / 1 : fix a case of wrap - around in sanity_check_meminfo ()
mfd : Fix return type of lm533 attribute is_visible
slub : Discard slab page when node partial > minimum partial number
fs : make inode_to_bdi () handle NULL inode
slab : fix alien cache handling
block / partitions / aix . c : replace count * size kzalloc by kcalloc
iommu / exynos : Add / remove callbacks should fail if no iommu is available
mfd : cros_ec : Instantiate ChromeOS EC character device
cyclades : fix read buffer overflow
aoe : error printed 1 too early
[ PATCH ] Fix typo in x86_64 __build_write_lock_const assembly
kernel / sys . c : fix missing rcu protection for sys_getpriority ()
[ PATCH ] gss_spkm3 : fix error handling in module init
atm : deal with setting entry before mkip was called
ipc / shm . c : check for overflows of shm_tot
drm / atomic : Wait for vblank whenever a plane is added to state .
vmwgfx : return - EFAULT if copy_to_user fails
clk : mvebu : fix setting unwanted flags in CP110 gate clock
mm : vma_adjust : remove superfluous check for next not NULL
pty : Fix packet mode setting race
[ TCP ] FRTO : Response should reset also snd_cwnd_cnt
staging : comedi : prevent auto - unconfig of manually configured devices
[ PATCH ] i386 : fix flat mode numa on a real numa system
KVM : fix cpuid E2BIG handling for extended request types
unix : use wq_has_sleeper in unix_dgram_recvmsg
sfc : Use kzalloc () to ensure struct efx_spi_device is fully initialised
[ TCP ]: Fix TCP header misalignment
mac80211 : Don ' t buffer non - bufferable MMPDUs
hwmon : ( lm87 ) Fix vrm write operation
mailbox , pl320 - ipc : remove __init from probe function
usb : dwc3 : ep0 : move to CONFIGURED also on delayed status
cpufreq : intel_pstate : Fix setting max_perf_pct in performance policy
orangefs : avoid freeing a slot twice in wait_for_direct_io ()
KVM : VMX : Only reload guest msrs if they are already loaded
dsa : mv88e6xxx : Fix error handling in mv88e6xxx_set_port_state
tty / vt : Fix the memory leak in visual_init
staging : rtl8188eu : Use kcalloc instead of kzalloc
drm : Return EBUSY if the framebuffer is unbound when flipping .
ath9k : enable ext channel pulse detection
i40e : Fix a port VLAN configuration bug
ARM : S3C64XX : Remove unused CPU devices for Cragganmore
perf trace : Fix detection of script extension
ALSA : hdspm - Abort on unknown firmware revision
staging : ks7010 : change length type to unsigned
Fix a referral error Oops
x86 : fix MP_processor_info section mismatch warning
IPoIB : Clear ipoib_neigh . dgid in ipoib_neigh_alloc ()
futexes : Fix infinite loop in get_futex_key () on huge page
ASoC : atmel - pdmic : add missing clk_disable_unprepare () on error in atmel_pdmic_cpu_dai_startup ()
greybus : loopback : Send some sensible data
clk : max77686 : fix number of clocks setup for clk_hw based registration
staging : rts5208 : Unnecessary parentheses cleanup
bnx2x : Add known PHY type check
ath9k : Check virtual wiphy state on tx ()
Btrfs : adjust NULL test
tty : fix vt_compat_ioctl
fscrypt : use EEXIST when file already uses different policy
acpi - cpufreq : Do not load on K8
perf tools : Try to lookup kernel module map before creating one
sfc : Really allow RX checksum offload to be disabled
r8152 : check RTL8152_UNPLUG for rtl8152_close
[ POWERPC ] spufs : fix another off - by - one bug in spufs_mbox_read
device - dax : check devm_nsio_enable () return value
mtd : docg3 : fix debug log verbosity
be2net : Check for POST state in suspend - resume sequence
inetpeer : initialize -> redirect_genid in inet_getpeer ()
[ PATCH ] 2 . 6 . 17 missing a call to ieee80211softmac_capabilities from ieee80211softmac_assoc_req
ASoC : Samsung : Add I2S S / W RST in startup function
ARM : imx : ehci : use void __iomem * to hold i / o addresses
iwlwifi : mvm : BT coex - fix BT prio for probe requests
USB : serial : digi_acceleport : fix OOB - event processing
USB : g_mass_storage : min (...) warning fixed
[ media ] s5p - jpeg : remove stray call to readl
Staging : sep : potential buffer overflow in ioctl
libata : add SATA PMP revision information for spec 1 . 2
spi_mpc8xxx : Fix uninitialized variable
drm / radeon : add backlight quirk for hybrid mac
ASoC : rt298 : fix null deref on acpi driver data
[ media ] cx23885 - dvb : check if dvb_attach () succeded
ceph : fix ack counter reset on connection reset
ALSA : hda - Clear left - over hp_pins in snd_hda_parse_pin_def_config ()
mac80211 : fix check for buffered powersave frames with txq
virtio : console : We support only one device at a time
pkt_sched : Add queue stopped test back to qdisc_run ().
cifs : always do is_path_accessible check in cifs_mount
iommu / vt - d : Check the return value of iommu_device_create ()
x86 / amd - iommu : Don ' t detach device from pt domain on driver unbind
inkern : iio_device_put after incorrect return / goto
uas : Fix bounds check in uas_find_endpoints
ASoC : Intel : Skylake : Initialize NHLT table
KVM : s390 : kvm / sigp . c : fix memory leakage
drm / i915 : Don ' t access snooped pages through the GTT ( even for error capture )
perf bench : Fix memcpy benchmark for large sizes
[ S390 ] s390 / setup : set default preferred console device " ttyS "
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
wan / fsl_ucc_hdlc : remove reduplicative freed memory ' uhdlc_priv '
drm / i915 / dp : Ratelimit DP aux timeout messages
ARM : 7772 / 1 : Fix missing flush_kernel_dcache_page () for noMMU
sctp : kzalloc () error handling on deleting last address
staging : rtl8712 : Replace kmalloc with kmalloc_array
ALSA : line6 : Give up on the lock while URBs are released .
drm / rockchip : Shutdown all crtcs when unbinding drm
mtd : mtd - abi : add a helper to detect the nand type
tipc : fix variable dereference before NULL check
mac80211 : fix register_hw error path
qlcnic : Initialize dcbnl_ops before register_netdev
staging : android : sync : Fix memory corruption in sync_timeline_signal ().
ALSA : asihpi : incorrect range check
orinoco : fix TKIP countermeasure behaviour
dma : timb_dma : Fix compiler warning
Staging : hv : channel : export vmbus_close to modules
mmc : sd : warn if card stays busy during init
carl9170 : fix typo in PS code
drm / i915 : Skip stolen region initialisation if none is reserved
drm / nouveau / bios / gp104 : initial support
blk - stat : fix blk_stat_sum () if all samples are batched
x86 / PCI : don ' t bother with root quirks if _CRS is used
USB : Remove unnecessary casts of void ptr returning alloc function return values
drm / amdgpu : add zero timeout check in amdgpu_fence_wait_seq_timeout
tcm_fc : Fix crash seen with aborts and large reads
ecryptfs : initialize new auth_tokens before teardown
ARM : 6891 / 1 : prevent heap corruption in OABI semtimedop
vfs : abort dedupe loop if fatal signals are pending
drm / amdgpu : bypass RLC init for SRIOV
virtio : console : fix race in port_fops_open () and port unplug
rbd : fix a memory leak in rbd_get_client ()
UBI : bugfix : dont oops with NULL module parameter
[ IA64 ] SN2 : security hole in sn2_ptc_proc_write
gpio : moxart : Use devm_gpiochip_add_data () for gpio registration
iommu / amd : Fix BUG when faulting a PROT_NONE VMA
Staging : wilc1000 : Replace memset with eth_zero_addr
sysfs : Make poll behaviour consistent
[ ALSA ] hdsp - Fix zero division
spi : davinci : do not use DMA for vmalloc ' ed buffers
kvm : irqchip : fix memory leak
[ media ] stb0899 : sign of CRL_FREQ doesn ' t depend on inversion
[ MTD ] Initialise s_flags in get_sb_mtd_aux ()
mm : fail prefaulting if page table allocation fails
iwlagn : save the latest smps mode
crypto : caam - Fix first parameter to caam_init_rng
trace / kprobes : Add back warning about offset in return probes
staging : vt6655 : Use alloc_etherdev () for kzalloc
IIO : Documentation : generic_buffer example : Avoid NULL pointer dereference
ARM : at91 : fix nommu build regression
qla2xxx : fix busy wait regression
dmaengine : edma : Correct the handling of src / dst_maxburst == 0
drivers : misc : pass miscdevice pointer via file private data
ocfs2 : send SIGXFSZ if new filesize exceeds limit - v2
[ media ] tm6000 : Don ' t try to use a non - existing interface
nvmet : Use direct IO for writes
[ ARM ] 4233 / 1 : nand / s3c2410 . c : warning fix
ASoC : s6000 : Use WARN_ON () instead of BUG_ON ()
fcoe : ensure that skb placed on the fip_recv_list are unshared
mfd : ab8500 - debugfs : Apply a check for - ENOMEM after allocating memory for sysfs
block : ioctl : fix information leak to userland
staging : unisys : visornic : remove driver version from visornic_main . c
perf tools : Fix build breakage on arm64 targets
clk : at91 : Fix memory leak in of_at91_clk_master_setup ()
ARM : 5623 / 1 : Treo680 : ir shutdown typo fix
dma : tegra20 - apbdma : err message correction
clk : hisilicon : Use kcalloc () in hisi_clk_init ()
fix pgoff in " have to relocate " case of mremap ()
ocfs2 : avoid a pointless delay in o2cb_cluster_check ()
[ IA64 ] missing exports hwsw_sync_ ...
kernel / irq / irqdomain . c : before use ' irq_data ', need check it whether valid .
KVM : PPC : Add emulation for dcba
cgroup : set ' start ' with the right value in cgroup_path .
dmaengine / ste_dma40 : fix introduced warnings
drm / nouveau / bus / gp104 : initial support
ATM : mpc , fix use after free
xfrm : Don ' t allow esn with disabled anti replay detection
dm cache : do not wake_worker () in free_migration ()
f2fs : fix inconsistency of block count during recovery
xhci : Warn when hosts don ' t halt .
pinctrl : pinctrl_select_state : set the old_state back on error
[ PATCH ] Bluetooth : fix potential NULL ptr deref in dtl1_cs . c :: dtl1_hci_send_frame ()
tcp_output : fix sparse warning for tcp_wfree
tty : Deprecate ldisc . chars_in_buffer () method
bridge : provide a mtu () method for fake_dst_ops
gpiolib : Fix NULL pointer deference
mfd : Pass htc - pasic3 led platform data through the cell platform_data
rtl8150 : rtl8150_disconnect (...) does not need tasklet_disable (...)
tg3 : Set initial carrier state to off .
cgroups : make unlock sequence in cgroup_get_sb consistent
Staging : gs_fpgaboot : Replace ' int32_t ' with ' int '.
perf symbols : Properly align symbol_conf . priv_size
drm / i915 : make sparse happy for the new vlv mmio read function
[ IPSEC ] Set byid for km_event in xfrm_get_policy
[ media ] media : davinci : vpif_display : drop unused member fbuffers
ARM : msm : Fix section mismatches in proc_comm . c
Btrfs : don ' t leak reloc root nodes on error
caif - hsi : Add missing return in error path
drm : Check if the allocation has succeeded before dereferencing newmode
iio : ad5755 : fix off - by - one on devnr limit check
[ media ] s5h1420 : fix a buffer overflow when checking userspace params
[ PATCH ] i2c : i2c - ibm_iic add I2C_CLASS_HWMON
HID : logitech - hidpp : add boundary check for name retrieval
tty : fix race in tty_fasync
dnet : Support the get_ts_info ethtool method .
mac80211 : remove a bit of dead mesh code
brcmfmac : rework pointer trickery in brcmf_proto_bcdc_query_dcmd ()
Input : regulator - haptic - fix module autoload for OF platform driver
ipr : Driver version 2 . 6 . 2
thinkpad_acpi : Fix a memory leak during module exit
[ AF_KEY ]: Fix bug in spdadd
drm / i915 : Make aliasing a 2nd class VM
usb : otg : Make USB3319 ULPI ID generic
HSI : nokia - modem : fix error handling of irq_of_parse_and_map
ALSA : fm801 : explicitly free IRQ line
i40e : add check for null VSI
mac80211 : avoid aggregation for VO traffic
ixgbe : in shutdown , do netif_running () under rtnl_lock
PCI : keystone - dw : Update PCI config space remap function
Btrfs : init inode ordered_data_close flag properly
mmc : sdhci - bcm2835 : Clean up platform allocations if sdhci init fails .
blk - mq : export blk_mq_free_request ()
[ SCSI ] ibmvfc : Fix DMA mapping leak on memory allocation failure
i2c : omap : on -> remove () call pm_runtime_put_sync ()
staging : lustre : osc : osc_extent_wait () shouldn ' t be interruptible
chelsio : error path fix
drm / radeon : fix sad_count check for dce3
[ PATCH ] dm table split_args : handle no input
reiserfs : Don ' t call reiserfs_get_acl () with the reiserfs lock
staging : unisys : visorchannel : Place logical continuation at the end of a line
[ media ] dvb - usb - gp8psk : Fix tuner timeout ( against git )
drm / i915 / gvt / kvmgt : remove some dead code
f2fs : set dirty state for filesystem only when updating meta data
brcm80211 : Don ' t leak ' vbuffer ' in brcmf_sdbrcm_write_vars ()
spidernet : node - aware skbuff allocation
gpio : GPIOHANDLE_GET_LINE_VALUES_IOCTL : Fix information leak
vhost : copy_to_user -> __copy_to_user
arm64 : kernel : do not mark reserved memory regions as IORESOURCE_BUSY
[ PATCH ] Fix sys_migrate_pages : Move all pages when invoked from root
jme : Fix VLAN memory leak
r8152 : check tx agg list before spin lock
macvtap : fix uninitialized return value macvtap_ioctl_set_queue ()
workqueue : Add quiescent state between work items
rfs : call sock_rps_record_flow () in tcp_splice_read ()
[ PATCH ] md : Avoid oops when attempting to fix read errors on raid10
btrfs : sysfs label interface should check for read only FS
staging : greybus : arche - platform : compress return logic into one line
spi : spi - fsl - dspi : Fix incorrect freeing of DMA allocated buffers
ALSA : hda - Update automute / automic upon jack retasking
Btrfs : Handle transid == 0 while opening devices
Btrfs : Fix test_range_bit for whole file extents
fsl_pq_mdio : Revive Gianfar TBI PHY support
[ PATCH ] x86 - 64 : remove prototype of free_bootmem_generic ()
staging : usbip : Fix deadlock
brcmfmac : fix possible memory leak
gpu : host1x : Check for valid host1x pointer
drm : fixes when i2c encoder slave mode_fixup is null .
USB : idmouse : fix NULL - deref at probe
ocfs2 : unmap_mapping_range () in ocfs2_truncate ()
powerpc / crypto / crc32c - vpmsum : Fix missing preempt_disable ()
staging : rts5208 : Spacing
iwlwifi : mvm : Avoid signal based decisions if ave beacon RSSI is 0
[ SCSI ] qla4xxx : correct return status in function qla4xxx_fw_ready
hwmon : ( coretemp ) fix oops on cpu unplug
sysfs : Remove double free sysfs_get_sb
coredump : make coredump_wait wait for mmap_sem for write killable
staging : usbip : userspace : libsrc : ( foo *) should be ( foo *)
block : Enable sysfs nomerge control for I / O requests in the plug list
powerpc / powernv / ioda2 : Gracefully fail if too many TCE levels requested
agp / intel : Use a non - reserved value for the cache field of the PTEs .
ARM : KVM : Bugfix : vgic_bytemap_get_reg per cpu regs
tty_audit : fix checkpatch complaint
drm / exynos : fimd : fix broken dp_clock control
[ media ] xc2028 : add missing break to switch
iio : adc : mcp320x : Set struct spi_driver . of_match_table
ASoC : Intel : Fix incorrect sizeof () in sst_hsw_stream_get_volume ()
xen - blkfront : remove IRQF_SAMPLE_RANDOM which is now a no - op
sched : Don ' t set sd -> child to NULL when it is already NULL
f2fs : avoid gc in cp_error case
staging : zcache : fix " zcache =" kernel parameter
mtd : elm : Use correct check on return value of pm_runtime_get_sync
mwl8k : prevent freeing free IRQ if ieee80211_register_hw () fails
[ media ] s5p - g2d : Remove casting the return value which is a void pointer
ASoC : Reset WM8962 with default ID value
tcp : zero ca_priv area when switching cc algorithms
i40evf : don ' t blow away MAC address
greybus : uart : Update line coding settings only when needed
staging : dwc2 : fix thinko in dwc2_hc_set_even_odd_frame ()
staging : iio : compress return logic
slob : fix memory corruption
Btrfs : fix bitmap size tracking
[ PATCH ] s390 : overflow in sched_clock
drivers / net / wireless / p54 / txrx . c Fix off by one error
drm : mxsfb : Fix crash when provided invalid DT bindings
ASoC : omap - pcm : off - by - one in ' omap_pcm_limit_supported_formats '
x86 / microcode / intel : Guard against stack overflow in the loader
ACPI : check acpi_disabled in acpi_table_parse () and acpi_table_parse_entries ()
r8169 : correct phy parameters for the 8110SC
vxlan : Update hard_header_len based on lowerdev when instantiating VXLAN
HID : hid - magicmouse : Add pointer and buttonpad properties for Magic Trackpad
ASoC : nuc900 - ac97 : fix a memory leak
m68k : Export empty_zero_page for ZERO_PAGE usage in modules .
cgroup : remove redundant code in cgroup_rmdir ()
usb : dwc3 : omap : use the macro we already have
xfrm : Fix truncation length of authentication algorithms installed via PF_KEY
spi : davinci : remove unnecessary completion variable initialization
perf probe : Fix the missed parameter initialization
mwl8k : declare MODULE_FIRMWARE
do_wait () wakeup optimization : child_wait_callback : check __WNOTHREAD case
OMAPDSS : add missing export for omap_dss_get_output ()
bna : ethtool : Avoid reading past end of buffer
selinux : fix setting of security labels on NFS
ASoC : simple - card : Add a NULL pointer check in asoc_simple_card_dai_link_of
virtio : delete vq from list
unicore32 / irq : Prepare puv3_gpio_handler for irq argument removal
trusted keys : Fix a memory leak in trusted_update ().
mm , hugetlb_cgroup : align hugetlb cgroup limit to hugepage size
ipv4 : netfilter : ip_tables : fix information leak to userland
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
atl1 : zero out CMB and SBM in atl1_free_ring_resources
powerpc / pci : export pcibios_free_controller ()
[ media ] tveeprom : add support for Xceive XC5000C tuner
sh_eth : Check for DMA mapping errors on transmit
ALSA : hda / realtek - Avoid invalid COEFs for ALC271X
drm / vmwgfx : Clear an uninitialized struct member
[ media ] soc - camera : change order of removing device
[ PATCH ] s3c2410fb : fix un - initialised dev field
mwifiex : update amsdu tx packet time stamp
tcp : Report mss_clamp with TCP_MAXSEG option in repair mode
[ CPUFREQ ] Longhaul - Fix power state test to do something more useful
[ media ] lirc : cannot read from tx - only device
ASoC : wm8750 : Replace direct snd_soc_codec dapm field access
staging : frontier : use after free in disconnect ()
usb : xhci - plat : properly handle probe deferral for devm_clk_get ()
[ PATCH ] md : bio leak fix
sis - agp : Remove SIS 760 , handled by amd64 - agp
x86 , PAT : Changing memtype to WC ensuring no WB alias
i2c : Align i2c_device_id
iwlwifi : fix PCI - E transport " inta " race
[ SPARC ]: Fix exec failures on sun4c .
drm / radeon : add a dpm quirk for all R7 370 parts
[ MTD ] [ OneNAND ] fix call to onenand_verify when writing subpages
ASoC : tlv320dac33 : Avoid powering off while in BIAS_OFF
usb : gadget : u_ether : fix dereference after null check coverify warning
perf kvm record / report : ' unprocessable sample ' error while recording / reporting guest data
scsi : ufs : qcom : Don ' t free resource - managed kmalloc element
ixgbe : Disallow device reset during ethtool test
cifs : potential missing check for posix_lock_file_wait
mtd : socrates_nand : Remove redundant dev_set_drvdata
drm / radeon / si_dpm : Limit clocks on HD86xx part
ch7006 : correctly handling failed allocation
batman - adv : randomize initial seqno to avoid collision
audit : log module name on delete_module
staging : tidspbridge : fix potential array out of bounds write
xfs : fix memory leak in xlog_recover_add_to_trans
USB : OHCI : don ' t allocate HCCA atomically
mlx4_core : Fix array overrun in dump_dev_cap_flags ()
Staging : batman - adv : Remove NULL pointer check
[ media ] em28xx - dvb : Prepare for si2157 driver getting more parameters
OMAP3 : PM : fix lockdep warning caused by omap3_pm_init
[ NETFILTER ]: nf_conntrack_sip : clear address in parse_addr ()
fuse : fix SEEK_END incorrectness
ceph : fix memory leak due to possible dentry init race
ASoC : wm8904 - fix memory leaks
virt : vbox : fix memory leak in hgcm_call_preprocess_linaddr
drm : when queuing an event with NEXTONMISS , return queued sequence to userspace
[ PATCH ] uml : activate_fd : return ENOMEM only when appropriate
parisc : fix wrong page aligned size calculation in ioremapping code
dmaengine : rcar - dmac : use result of updated get_residue in tx_status
ASoC : ssm4567 : Reset device before regcache_sync ()
KVM : use heuristic for fast VCPU lookup by id
scsi : megaraid_sas : return error when create DMA pool failed
ext4 : disable COLLAPSE_RANGE for bigalloc
perf / ftrace : Fix paranoid level for enabling function tracer
userns : don ' t leak root user
arch / avr32 : Eliminate NULL test and memset after alloc_bootmem
mm : batch - free pcp list if possible
tproxy : Add missing CAP_NET_ADMIN check to ipv6 side
Btrfs : fix off - by - one in cow_file_range_inline ()
tokenring / 3c359 . c : Prevent possible mem leak when open failed
ceph : cleanup aborted requests when re - sending requests .
cifs : return error on invalid value written to cifsFYI
aio : prevent double free in ioctx_alloc
genirq , ARM : Fix boot on ARM platforms
hamradio / yam : fix info leak in ioctl
mv643xx_eth : potential null dereference
igmp : Avoid zero delay when receiving odd mixture of IGMP queries
[ ALSA ] Fix possible deadlocks in sequencer at removal of ports
Staging : bcm : add a missing break statement
ext4 : fix error paths in swap_inode_boot_loader ()
net : call newid / getid without rtnl mutex held
tridentfb : use display information in info not in var for panning
staging : fbtft : do not allocate huge txbuf
kernel / exit . c : avoid undefined behaviour when calling wait4 ()
[ media ] davinci : dm355 : Fix uninitialized variable compiler warnings
ALSA : seq : Fix missing NULL check at remove_events ioctl
drm / vc4 : plane : Expand the lower bits by repeating the higher bits
floppy : Add an extra bound check on ioctl arguments
x86 : don ' t send SIGBUS for kernel page faults
ARM : 7185 / 1 : perf : don ' t assign platform_device on unsupported CPUs
NFC : Clear pn533 target structure
USB : EHCI : add a name for the platform - private field
scsi : Fix use - after - free
mm : Adjust do_pages_stat () so gcc can see copy_from_user () is safe
staging : ozwpan : Return error , if PD is not connected .
dp83640 : Get calibration pin with ptp_find_pin
ASoc : wm8731 : add 32bit mode .
x86 / fpu : Don ' t cache access to fpu_fpregs_owner_ctx
greybus : bundle : use hexadecimal notation for class attribute
KVM : MCE : Add MCG_SER_P into KVM_MCE_CAP_SUPPORTED
rtlwifi : rtl8192ce : Set fw_ready flag
ibmvnic : Handle error case when setting link state
cdrom : Check SCSI passthrough support before reading audio
KVM : nSVM : Fix IOIO size reported on emulation
ipv4 : fib : Set offload indication according to nexthop flags
ARM : OMAP : rx51 : Fix a section mismatch warn
modedb : fix incorrect sync and vmode flags for CVT modes
USB : allow usbstorage to have LUNS greater than 2Tb
HID : i2c - hid : prevent buffer overflow in early IRQ
epoll : clear the tfile_check_list on - ELOOP
memstick : init sysfs attributes
pNFS / flexfiles : missing error code in ff_layout_alloc_lseg ()
staging : vt6655 : device_main : Replace NULL comparison with ! x
crypto : hash - Add AHASH_REQUEST_ON_STACK
drm / ttm : fix memory leak while individualizing BOs
cifs : ensure that vol -> username is not NULL before running strlen on it
USB : usbmon : use the new usb debugfs directory
[ media ] omap3isp : Handle omap3isp_csi2_reset () errors
soc : qcom : aoss : Fix missing put_device call in qmp_get
irda : smsc wait count reaches - 1
drm / amdkfd : Pass queue type to pqm_create_queue ()
staging : iio : dac : ad5686 : Check for negative values
ALSA : hda - fix the mic mute led problem for Latitude E5550
drm / amd / display : Disable cursor on video surface .
drm / nouveau / ltc / gv100 : initial support
ALSA : usb - audio : avoid freeing umidi object twice
target / iblock : Add checks for backend DIF emulation
cpufreq : Prevent problems in update_policy_cpu () if last_cpu == new_cpu
ALSA : snd - usb : Fix uninitialized variable usage
tty : goldfish : Fix a parameter of a call to free_irq
strip : Fix termios assumption
agp : info leak in agpioc_info_wrap ()
regmap : Check readable regs in _regmap_read
[ media ] hdpvr : Fix memory leak
cifs : fix possible memory corruption in CIFSFindNext
char / tpm : Fix unitialized usage of data buffer
phy : qcom - qusb2 : Fix crash if nvmem cell not specified
staging : comedi : cb_pcidas : check for failure of subdev_8255_init
mac80211_hwsim : require at least one channel
x86 , efi : Check table header length in efi_bgrt_init ()
ALSA : ali5451 : Fix out - of - bound position reporting
iwlwifi : nvm : don ' t print NVM section reads by default
omap3 : Fix booting if package is uninitialized
KVM : Don ' t accept obviously wrong gsi values via KVM_IRQFD
staging : goldfish : Call free_irq in error path
ASoC : atmel_ssc_dai : Match the CMR divider only in full duplex .
acpi : add checking for NULL early param
bus : arm - ccn : Do not attempt to configure XPs for cycle counter
ip6gretap : Fix MTU to allow for Ethernet header
xfrm_user : fix info leak in copy_to_user_policy ()
brcmfmac : clear eventmask array before using it
USB : usbfs : fix - ENOENT error code to be - ENODEV
[ media ] ov5642 : avoid calling ov5642_find_datafmt () twice
usb : storage : shuttle_usbat : fix discs being detected twice
block : handle blk_pm_resume_request () requests in blk_execute_rq_nowait ()
ALSA : usb - audio : WARN_ON when alts is passed as NULL
dmaengine : ioatdma : workaround SKX ioatdma version
mwifiex : Fix possible buffer overflows in mwifiex_cmd_802_11_ad_hoc_start
defxx . c : dfx_bus_init () is __devexit not __devinit
powerpc / powernv : Initialize DMA for PEs
xhci : really enqueue zero length TRBs .
drivers / usb / host / ehci - fsl . c : add missing iounmap
perf tools : Fix a memory leak in __open_dso ()
mac80211 : initialize power mode for mesh STAs
Input : ad7877 - filter events where pressure is beyond the maximum
staging : rtl8723au : collect_bss_info (): Don ' t memset () what we just kzalloc ()' d
KEYS : Fix termination condition in assoc array garbage collection
staging / xgifb : Remove unnecessary binary calculation in GetXG27FPBits
virtio_net : zero is an invald queue_pairs number
drm / amd / display : Add same check as reset pipes for programing backend regs .
mac80211 : fix uninitialized variable
USB : CDC ACM : Fix NULL pointer dereference
ARM : 6076 / 1 : SA1100 : add processor check to sa1110 - cpufreq driver
fscrypt : fix dereference of NULL user_key_payload
perf annotate : Handle lower case key code in annotate_browser__run ()
ath10k : fix WMI scan command length
fix irq flags in saa7134
[ SCSI ] pmcraid : reject negative request size
r8169 : switch to napi_schedule_irqoff
proc : fix oops on invalid / proc /< pid >/ maps access
[ PATCH ] struct path : convert ieee1394
[ PATCH ] uniput - fix crash on SMP
sched : re - tune NUMA topologies
ufs_extend_tail (): fix the braino in calling conventions of ufs_new_fragments ()
af_unix : don ' t append consumed skbs to sk_receive_queue
staging : mt7621 - pinctrl : add SPDX identifier
btrfs : fix in - memory value of total_devices after seed device deletion
crypto : eseqiv - Fix IV generation for sync algorithms
staging : comedi : jr3_pci : change module description
scsi : libsas : fix memory leak in sas_smp_get_phy_events ()
md : allow a partially recovered device to be hot - added to an array .
ath9k_hw : Fix tx power settings for AR9003
gpio : davinci : Fix possible NULL pointer deference
USB : serial : fix error message on close in generic driver
[ SCSI ] Fix uninitialised width and speed in sym2
drm / i915 : Fix user ptr check size in eb_relocate_vma ()
libata : ensure host is free ' d on error exit paths
iwlwifi : set size of ucode section
staging : slicoss : fix a leak when kzalloc fail
[ SCSI ] qla2xxx : Correct regression in relogin code .
drm / ttm : test for dma_address array allocation failure
Btrfs : add missing error handling to read_tree_block
ACPI : Add NULL pointer check in acpi_bus_start
rcu : add DEBUG_OBJECTS_RCU_HEAD check for alignment
ACPI : battery : Support for non - spec name for LiIon technology
staging : wilc1000 : fixes potential null dereference ' wid . val '
qla2xxx : Clear loop_id for ports that are marked lost during fabric scanning .
bpf : ixgbe : Report bpf_prog ID during XDP_QUERY_PROG
ax25 : Fix NULL pointer dereference in ax25_kill_by_device
USB : option : reduce interrupt - urb logging verbosity
ALSA : pcm : Call kill_fasync () in stream lock
Btrfs : fix memory leak in start_transaction ()
usbdev : BKL pushdown
rndis_wlan : add return value validation
bnxt_en : Do not allow VF to read EEPROM .
drm / amdgpu / gfx8 : specify which engine to wait before vm flush
AppArmor : fix oops in apparmor_setprocattr
fs / befs / linuxvfs . c : befs_iget (): remove unneeded initialization to NULL
[ IA64 ] Prevent sn2 ptc code from executing on all ia64 subarches
[ PATCH ] cciss : set default raid level when reading geometry fails
cxgb3 : Missing rtnl lock in error recovery
usb : gadget : don ' t release an existing dev -> buf
ARM : 7809 / 1 : perf : fix event validation for software group leaders
dm : fix the second dec_pending () argument in __split_and_process_bio ()
[ media ] ascot2e : Fix I2C message size check
ext4 : fix incorrect block reservation on quota transfer .
ceph : prevent i_version from going back
ftrace : iter ctrl fix
drm : Complete moving rotation property to core
lpfc : Fix internal loopback failure .
[ ALSA ] Fix errors with user TLV_WRITE
wl1271 : fix wl1271_spi driver name
xfs : fix inobt inode allocation search optimization
drm / radeon : only apply the SS fractional workaround to RS [ 78 ] 80
jbd2 : clear BH_Delay & BH_Unwritten in journal_unmap_buffer
scsi : fcoe : fix use - after - free in fcoe_ctlr_els_send
[ media ] [ v3 , media ] av7110 : check for negative array offset
fs : allow d_instantiate to be called with negative parent dentry
hp - wmi : add return value checking for input_allocate_device ()
ALSA : hda - Add mic - boost controls to ALC662 / 663 auto configuration
x86 : not clear empty_zero_page again
[ PATCH ] orinoco : Fix memory leak on error in processing hostscan frames .
iio : lmp91000 : Set parent device
[ media ] rtl28xxu : lower the rc poll time to mitigate i2c transfer errors
drm / nouveau / fbcon : fix NULL pointer access in nouveau_fbcon_destroy
Input : usb1400_ts - fix access to " device data " in resume function
tpm : Fix initialization of the cdev
KVM : async_pf : Add missing call for async page present
fuse : fix uninitialized flags in pipe_buffer
kvm : x86 : fix emulator buffer overflow ( CVE - 2014 - 0049 )
Staging : Octeon - usb : octeon - hcd . c return value
mctp : defer the kfree of object mdev -> addrs
powerpc / radix : Update MMU cache
drivers / cpufreq / pcc - cpufreq . c : avoid NULL pointer dereference
HID : roccat : Fixed false dpi reporting when using osd event
phy : ti - pipe3 : Fix suspend / resume and module reload
gma500 : mdfld_dsi_pkg_sender : fix - Wtype - limits warning
vmxnet3 : annotate hwaddr members as __iomem pointers
x86 , mm , hugetlb : Add missing TLB page invalidation for hugetlb_cow ()
drm / i915 : get / put runtime PM when we get / put a power domain
caif : Fix missing msg_namelen update in caif_seqpkt_recvmsg ()
fuse : check size of FUSE_NOTIFY_INVAL_ENTRY message
mfd : rtsx : Release IRQ during shutdown
sound / soc / fsl / p1022_ds . c : add missing of_node_put
udf : avoid info leak on export
ACPI : debugfs custom_method open to non - root
sysctl : Drop reference added by grab_header in proc_sys_readdir
x86 / microcode / intel : Fix memleak in save_microcode_patch ()
btrfs : verbose error when we find an unexpected item in sys_array
PCI : Fix devm_pci_alloc_host_bridge () memory leak
libnvdimm , btt : check memory allocation failure
igb : add i211 to i210 PHY workaround
ALSA : HDA : Fixup Realtek headphone pin initialization
mtd : spear_smi : clear status register on init
ath9k : reinitialize block ack window data when starting aggregation
ARM : OMAP : Use kcalloc () in omap_system_dma_probe ()
staging : gdm724x : redundant variables idProduct and idVendor
drm : i915 : Explicitly apply PWM config extracted from pwm_args
KVM : nVMX : Validate the IA32_BNDCFGS on nested VM - entry
KVM : arm / arm64 : timer : Check for properly initialized timer on init
btrfs : send : use kvmalloc in iterate_dir_item
ipv6 : don ' t use tw net when accounting for recycled tw
x86 / mce / AMD : Make the init code more robust
phy : tegra : xusb : Fix return value of tegra_xusb_find_port_node function
iio / adc : ( max1363 ) Provide OF node information to iio device
cnic : call cp -> stop_hw () in cnic_start_hw () on allocation failure
staging : intel_sst : Fix memory leak
drm / i915 : Restore marking context objects as dirty on pinning
x86 / microcode / AMD : Drop redundant printk prefix
ring - buffer - benchmark : Fix the wrong type
userfaultfd : hugetlbfs : remove superfluous page unlock in VM_SHARED case
iwlwifi : mvm : don ' t allow sched scans without matches to be started
xen / evtchn : Fix name of Xen event - channel device
KVM : powerpc : fix init / exit annotation
input : pmic8xxx - pwrkey : Do not use mfd_get_data ()
drm / omap : verify that display x - res is divisible by 8
USB : serial : omninet : fix reference leaks at open
blackfin : bf609 : let clk_disable () return immediately if clk is NULL
dm ioctl : fix out of bounds array access when no devices
perf : Initialize callchains roots ' s childen hits
Staging : rtl8192e : Fix potential NULL pointer dereference
ip6erspan : make sure enough headroom at xmit .
ipv6 : fix icmp6_dst_alloc ()
dccp / tcp : do not inherit mc_list from parent
sky2 : print Optima chip name
[ media ] solo6x10 : free DMA allocation when releasing encoder
[ POWERPC ] spufs : Don ' t yield nosched context
drm / nv50 / disp : allow interlaced and doublescan modes on digital outputs
ARM : DRA7 / AM43XX : fix header definition for omap44xx_restart
pinctrl : sunxi : check clk_prepare_enable return value
mtd : nand : don ' t free the global data too early
[ XFS ] Fix a mutex_destroy diagnostic about a locked - mutex - on - destroy from
brcmfmac : initialize escan function pointer during scheduled scan
mm : memcg : Correct unregistring of events attached to the same eventfd
fcaps : clear the same personality flags as suid when fcaps are used
[ PATCH ] Dynamic kernel command - line : m68k
usb : otg : twl4030 - usb : Fix unbalanced regulator disables at module removal
staging : android : ion : Return an ERR_PTR in ion_map_kernel
ext4 : prevent stack overrun in ext4_file_open
KVM : x86 : Improve thread safety in pit
drm / amd / amdgpu : fix over - bound accessing in amdgpu_cs_wait_any_fence
ext4 : avoid hang when mounting non - journal filesystems with orphan list
amdkfd : fence_wait_timeout () can be static
iommu : Stop deferring probe at end of initcalls
futexes : Fix infinite loop in get_futex_key () on huge page
[ media ] cx23885 : Silence unknown command warnings
KVM : MMU : invalidate and flush on spte small -> large page size change
perf symbols : Only un - prelink non - zero symbols
ASoC : rt5677 : Reintroduce I2C device IDs
perf : Treat attr . config as u64 in perf_swevent_init ()
bnx2x : VF ndo sanity
[ GFS2 ] re - support special inode
rds : prevent dereference of a NULL device
staging : lustre : race condition for check / use cfs_fail_val
drm / i915 : Disable gpu reset on i965g / gm
uas : Fix bounds check in uas_find_endpoints
ASoC : Intel : Skylake : Initialize NHLT table
ipv6 : sr : fix out - of - bounds access in SRH validation
amd - xgbe : fix potential memory leak in xgbe - debugfs
greybus : interface : fix double freeing of interface structure
Btrfs : fix missing error handler if submiting re - read bio fails
ALSA : hda - Make azx_attach_pcm_stream static
powerpc : Fix handling of alignment interrupt on dcbz instruction
floppy : don ' t write kernel - only members to FDRAWCMD ioctl output
USB : ehci : fix debugfs ' lpm ' permissions
mac80211 : fix fragmentation code , particularly for encryption
drm / i915 : Disable tiling on IGDNG for now
IPoIB : Do not print error messages for multicast join retries
uwb : uwbd () is not freezable kthread
staging : unisys : visorbus : Check controlvm message payload size
clk : versatile : free icst on error return
usb : chipidea : Allow disabling streaming not only in udc mode
net : avoid NULL deref in inet_ctl_sock_destroy ()
gpio : moxart : Use devm_gpiochip_add_data () for gpio registration
[ PATCH ] irq - devres : fix failure path of devm_request_irq ()
viafb : remove LVDS initialization
kvm : irqchip : fix memory leak
atmel_spi : reload RCR before TCR
Input : twl4030_keypad - pass correct pointer to free_irq ()
[ PATCH ] s390 : chps [] array too short
pata_atiixp : fix trivial indentation misalignment on if statement
staging : ks7010 : use u16 instead of unsigned short in hostif_data_indication
drivers : misc : pass miscdevice pointer via file private data
usb : misc : legousbtower : Fix memory leak
sctp : better integer overflow check in sctp_auth_create_key ()
libata : initialize qc -> dma_dir to DMA_NONE
sctp : do not inherit ipv6_ { mc | ac | fl } _list from parent
xen / events : prevent calling evtchn_get on invalid channels
i40e : Add missing braces to i40e_dcb_need_reconfig ()
drm / radeon / kms : Fix NULL ptr dereference
ieee802154 : ca8210 : prevent memory leak
gro : Fix kcalloc argument order
iwlwifi : dbg_ini : fix memory leak in alloc_sgtable
[ PATCH ] Audit : Fix the format type for size_t variables
fs / compat_ioctl . c : VIDEO_SET_SPU_PALETTE missing error check
perf sched replay : Support using - f to override perf . data file ownership
x86 : fix warning on 32 - bit non - PAE
io_uring : check that we have a file table when allocating update slots
ubifs : Use kmalloc_array ()
i2c : xiic : Remove the Addressed as slave interrupt
pinctrl : samsung : Handle memory allocation failure during wakeup banks init
sctp : keep fragmentation point aligned to word size
drm / nouveau : require contiguous bo for framebuffer
Btrfs : disk - io . c ( open_ctree ): avoid leaks upon allocation failure
sctp : avoid BUG_ON on sctp_wait_for_sndbuf
atl2 : Disable unimplemented scatter / gather feature
ext4 : fix memory leak in ext4_insert_range ()
loopback : off by one in tcm_loop_make_naa_tpg ()
percpu : use notrace variant of preempt_disable / preempt_enable
[ PATCH ] fs / smbfs / proc . c : fix data corruption in smb_proc_setattr_unix ()
ALSA : hda - Update automute / automic upon jack retasking
io_uring : fix uninitialized field in rw io_kiocb
[ media ] v4l : async : check for v4l2_dev in v4l2_async_notifier_register ()
mfd : rave - sp : Initialize flow control and parity of the port
mlxsw : spectrum_router : Fix error path in mlxsw_sp_router_init
tcp : drop SYN + FIN messages
drm / i915 : Do no use kfree () to free a kmem_cache_alloc () return value
Input : ad7879 - pass up error codes from probe functions
perf top : Fix -- delay_secs 0 division by zero
atm : update msg_namelen in vcc_recvmsg ()
mtip32xx : fix potential crash on SEC_ERASE_UNIT
chipidea : udc : free pending TD at removal procedure
bluetooth : Validate socket address length in sco_sock_bind ().
reset : Fix of_reset_control_get () for consistent return values
getname_kernel () needs to make sure that -> name != -> iname in long case
i2c : designware : Use transfer timeout from ioctl I2C_TIMEOUT
i40e : prevent memory leak in i40e_setup_macvlans
fscrypto : only allow setting encryption policy on directories
x86 / microcode / intel : Guard against stack overflow in the loader
iio : adc : mcp3422 : Checking for error on probe
powerpc / perf : Fix IMC initialization crash
drm / i915 : Expand DPF support to Haswell
dm raid1 : keep retrying alloc if mempool_alloc failed
[ PATCH ] USB : resolve Zaurus problem
USB : pl2303 : fix return value of tiocmset
sdio : handle null tuples
rtc : ac100 : Add NULL checking for devm_kzalloc call
ceph : check mapping to determine if FILE_CACHE cap is used
ipv6 : Fix ip6_xmit to send fragments if ipfragok is true
[ media ] v4l2 - ioctl : set readbuffers to 2 in g_parm
mnt : release locks on error path in do_loopback
iio : adis16400 : Report pressure channel scale
perf tools : callchain : Fix spurious ' perf report ' warnings : ignore empty callchains
[ XFS ] Make xfsbufd threads freezable
ASoC : Check for NULL register bank in snd_soc_get_cache_val ()
staging : frontier : use after free in disconnect ()
pinctrl / amd : Prepare amd_gpio_irq_handler for irq argument removal
powerpc / 64s / hash : Allow MAP_FIXED allocations to cross 128TB boundary
UBIFS : avoid kernel error if ubifs superblock read fails
usb : udc : core : fix error handling
[ SCSI ] scsi_lib : Fix bug in completion of bidi commands
i2400m : keep index within ms_to_errno []
r8a66597 - hcd : use USB_DT_HUB
[ media ] usbtv : correctly handling failed allocation
tty / serial : at91 : fix race condition in atmel_serial_remove
drm / i915 / gvt : Do not use I915_NUM_ENGINES to iterate over the mocs regs array
signal : always clear sa_restorer on execve
[ PATCH ] cpufreq_conservative : alter default responsiveness
[ S390 ] qdio : output queue stall on FCP and network devices
ssb : cc : use correct min_msk for 0x4312
Btrfs : clear uptodate flags of pages in sys_array eb
staging : sbe - 2t3e3 : Fix possible reuse of freed memory in timer function
quota : info leak in quota_getquota ()
fsl / fman : MEMAC may use QSGMII PHY interface mode
staging : wilc1000 : Revert " staging : wilc1000 : remove spaces around '->'"
[ Bluetooth ] Ignore additional interfaces of BPA 100 / 105 devices
[ NETFILTER ]: nf_conntrack_pptp : fix NAT setup of expected GRE connections
Phonet : refuse to send bigger than MTU packets
staging , rts5208 , LLVMLinux : Change extern inline to static inline
staging : vt6655 : device_rx_srv check sk_buff is NULL
mixart : Add missing vmalloc . h include
rcuclassic : fix compiler warning
USB : gadget : validate interface OS descriptor requests
drm / i915 : Start WM computation from scratch on ILK - BDW
ALSA : hda - Add docking station support for another HP machine
drm / etnaviv : ( re -) protect fence allocation with GPU mutex
xfs : disallow ro -> rw remount on norecovery mount
crypto : drbg - fix failure of generating multiple of 2 ** 16 bytes
KVM : s390 : fix assumption for KVM_MAX_VCPUS
[ SCSI ] scsi_scan . c : Release mutex in error handling code
powerpc / spufs : Remove invalid semicolon after if statement
tracing : Fix freeing of filter in create_filter () when set_str is false
PM : Free memory bitmaps if opening / dev / snapshot fails
[ PATCH ] sysfs : zero terminate sysfs write buffers
ipw2100 : declare MODULE_FIRMWARE
KVM : x86 : fix for buffer overflow in handling of MSR_KVM_SYSTEM_TIME ( CVE - 2013 - 1796 )
arch / tile : fix reversed test of strict_strtol () return value
rtnl : fix info leak on RTM_GETLINK request for VF devices
x86 : remove set_fixmap () warning
drm / i915 : fix integer overflow in i915_gem_execbuffer2 ()
ax25 : Fix UAF bugs in ax25 timers
drm / ttm : fix memory leak noticed by kmemleak .
drm / i915 / cnl : Reprogram DMC firmware after S3 / S4 resume
pcie port driver : correctly detect native PME feature
llc : Fix missing msg_namelen update in llc_ui_recvmsg ()
e1000e : do not attempt to disable smart powerdown if not supported
proc : prevent accessing / proc /< PID >/ environ until it ' s ready
gpu : host1x : clk_round_rate () can return a zero upon error
rose : fix info leak via msg_name in rose_recvmsg ()
dac960 : return success instead of - ENOTTY
perf tools : Do not accept parse_tag_value () overflow
tpm : fix a kernel memory leak in tpm - sysfs . c
usb : phy : msm : cast to enum msm_usb_phy_type
xfs : add missing rmap error return
batman - adv : Only write requested number of byte to user buffer
dccp : Empty the write queue when disconnecting
crypto : user - fix memory leak in crypto_report
ASoC : sgtl5000 : Improve the error message on slave mode setting
eCryptfs : Remove buggy and unnecessary write in file name decode routine
xen : fix memory leak in __xen_pcibk_add_pci_dev ()
ath9k_htc : Protect ampdu_action with a mutex
usb : xhci : Fix OOPS in xhci error handling code
atm : fix info leak in getsockopt ( SO_ATMPVC )
KVM : x86 : apply kvmclock offset to guest wall clock time
ipv6 : sit : update mtu check to take care of gso packets
tty : drop the pty lock during hangup
vhost : fix length for cross region descriptor
drm / i915 : Defend against userspace creating a gem object with size == 0
KVM : x86 : inject exceptions produced by x86_decode_insn
brcmfmac : avoid potential stack overflow in brcmf_cfg80211_start_ap ()
perf / x86 / intel / uncore : Correct fixed counter index check for NHM
fs : umount on symlink leaks mnt count
exofs : Remove redundant unlikely ()
fs_enet : Don ' t receive packets when the napi budget == 0
drm / msm : Call drm_prime_gem_destroy to clean up imported GEM object
leds - atmel - pwm . c : Make pwmled_probe () __devinit
USB : uas : Fix slave queue_depth not being set
i386 : arch / x86 / math - emu / fpu_entry . c warning fix
mm : add ' try_get_page ()' helper function
[ CRYPTO ] scatterwalk : Handle zero nbytes in scatterwalk_map_and_copy
clk : tegra : Use definition for pll_u override bit
[ SCSI ] isci : fix missed unlock in apc_agent_timeout ()
[ media ] smsusb : fix the default delivery system setting
xfs : trivial indentation fixup for xfs_iext_remove_node
amd - xgbe : Update connection validation for backplane mode
staging : android : Break up a long line in binder_send_failed_reply
crypto : qat - update init_esram for C3xxx dev type
crypto : inside - secure - fix an error handling path in safexcel_probe ()
KVM : x86 : Fix potential divide by 0 in lapic ( CVE - 2013 - 6367 )
KVM : x86 : fix error return code in kvm_arch_vcpu_init ()
powerpc / mm : Fix hash table dump when memory is not contiguous
wil6210 : fix check for sparrow D0 FW file
drm / sun4i : Fix uninitialized variables in vi layer
iommu / amd : Free domain id when free a domain of struct dma_ops_domain
blk - mq : fix allocation of set -> tags
powerpc / ptrace : Fix brk . len used uninitialised
ASoC : DaVinci : pcm , constrain buffer size to multiple of period
[ media ] coda : v4l2 - compliance fix : zero pixel format priv field
drm : Shift wrap bug in create_in_format_blob ()
greybus : audio : return success for stop trigger if device removed
HID : picolcd : sanity check report size in raw_event () callback
ALSA : Fix a Oops bug in omap soc driver .
ARM : stm32 : Fix copyright
batman - adv : fix memory leak when dropping packet from other gateway
crypto : blkcipher - Do not use eseqiv on stream ciphers
mac80211 : properly free requested - but - not - started TX agg sessions
bnxt_en : Check for zero length value in bnxt_get_nvram_item ().
wm831x_power : Add missing kfree ( wm831x_power ) in wm831x_power_remove ()
drm / virtio : don ' t leak bo on drm_gem_object_init failure
iwlwifi : mvm : fix possible memory leak
brcmfmac : correct setting of WEP broadcast / unicast keys
drm / i915 : Drain any freed objects prior to hibernation
ALSA : correct kcalloc usage
af_key : Do not call xfrm_probe_algs in parallel
ath10k : fix sending wmi cmd during the tdls teardown
iio : health : max30102 : Check retval of powermode function
dm flakey : check for null arg_name in parse_features ()
block : blk_init_allocated_queue () set q -> fq as NULL in the fail case
sctp : Fix sk_ack_backlog wrap - around problem
staging : unisys : avoid format string parsing
xfs : fix xfs_error_get_cfg for negative errnos
bonding : disallow enslaving a bond to itself
cpuidle : delay enabling interrupts until all coupled CPUs leave idle
spi / ppc4xx : Use kcalloc () in spi_ppc4xx_of_probe ()
crypto : tcrypt - fix uninit sg entries in test_acipher_speed
ipv6 : do not free rt if FIB_LOOKUP_NOREF is set on suppress rule
sysctl : check length at deprecated_sysctl_warning
[ SCSI ] libsas : remove expander from dev list on error
ath10k : enable channel 144 on 5GHz band
sh64 : re - add the __strnlen_user () prototype
rtc : sun6i : ensure rtc is kfree ' d on error
vfio / spapr_tce : Check kzalloc () return when preregistering memory
[ SCSI ] zfcp : Fix error checking for ELS ADISC requests
jbd : Check return value of __getblk ()
cpufreq : st : enable selective initialization based on the platform
gpio - pch : cleanup __iomem annotation usage
mac80211 : mesh only parameter mppath maybe unused
rpmsg : glink : Initialize the " intent_req_comp " completion variable
perf intel - pt : Fix missing stack clear
tracing / filters : don ' t remove old filters when failed to write subsys -> filter
mtd : nand : sharpsl : Register partitions using the parsers
drm / i915 / gvt : Disable access to stolen memory as a guest
[ PATCH ] readahead : reset cache_hit earlier
ALSA : sound / isa / opti9xx / miro . c : eliminate possible double free
USB : serial : upd78f0730 : sort device ids
cuse : fix memory leak
mac80211 : Fix ibss station got expired immediately
ASoC : fsi : constify dev_pm_ops structure
spi : core : Fix check for dma_map_sg () failures
Btrfs : fix btrfs_search_slot_for_read backwards iteration
ext4 : Fix ext4_ext_journal_restart ()
btrfs : fix wrong max system array size check in kernel space
TOMOYO : Fix oops in tomoyo_mount_acl ().
ALSA : HDA : Fix volume control naming for surround speakers on Realtek auto - parser
Btrfs : fix wrong extent mapping for DirectIO
iwlwifi : mvm : request statistics when reading debugfs
drm / tegra : Use lockless gem BO free callback
tools / liblockdep : Fix comparison of a boolean value with a value of 2
[ IPV6 ]: Fix slab corruption running ip6sic
[ ALSA ] hda - codec - Fix unbalanced mutex
jbd2 : preserve original nofs flag during journal restart
futex : Prevent overflow by strengthen input validation
ASoC : dapm : Make sure all dapm contexts are updated
ipw2200 : fix error return code in ipw_load ()
KVM : X86 : Don ' t use PV TLB flush with dedicated physical CPUs
aoe : Fix OOPS after SKB queue changes .
fuse : destroy bdi on error
drm / bridge : adv7511 : Fix a use after free
ath9k : Fix TX power calculation
ASoC : rockchip : i2s : fix unbalanced clk_disable
staging : gdm72xx : Condense two statements into one to improve code readability .
drm / nouveau / mmu / gp104 : initial support
staging / lustre / llite : Fix integer overflow in ll_fid2path
cxgb4vf : Fail open if link_start () fails .
Drivers : w1 : remove last __devexit_p () instance
mm : hugetlb : fix clear_huge_page ()
hwmon : ( ina2xx ) don ' t accept shunt values greater than the calibration factor
perf tools : Fix new_term () missing free on error path
[ X25 ]: Restore skb -> dev setting in x25_type_trans ().
rxrpc : Fix oops when discarding a preallocated service call
module : Don ' t report discarded init pages as kernel text .
Input : ams_delta_serio - fix wrong kfree in ams_delta_serio_exit
ALSA : timer : Fix double unlink of active_list
ovl : fix dentry reference leak
[ media ] dvbdev : replace kcalloc with kzalloc
panic , vt : do not force oops output when panic_timeout < 0
sunvnet : fix NULL pointer dereference
microblaze : Remove duplicated prototype of start_thread ()
mac80211 : use RCU read locks for sta_info_get
mdio_bus : Fix use - after - free on device_register fails
mwifiex : fix a reversed condition
tpm / st33zp24 / spi : Improve st33zp24_spi_evaluate_latency
page_cgroup should ignore empty nodes
exit : proc : don ' t try to flush / proc / tgid / task / tgid
x86 : fix broken flush_tlb_others_ipi ()
usb : otg : mxs - phy : add missing type to usb_phy
rapidio / tsi721 : fix incorrect detection of address translation condition
mac80211 : fix AP - mode frame matching
sm501fb : deallocate colormap only if allocated
staging : sep : add missing misc_deregister () on error in sep_register_driver_with_fs ()
virt : vbox : Only copy_from_user the request - header once
ath10k : fix firmware assert in monitor mode
[ PATCH ] Char : serial167 , remove useless tty check
pinctrl : sx150x : handle missing ' advanced ' reg in sx1504 and sx1505
drm / amd / display : fix uninitialized variable warning
x86 : fix pci - gart failure handling
cfg80211 : initialize sinfo in cfg80211_get_station
KVM : Align cr8 threshold when userspace changes cr8
igb : add conditions for I210 to generate periodic clock output
be2iscsi : add an missing goto in error path
PCI : Don ' t leak memory if sysfs_create_bin_file () fails
soc / tegra : pmc : Ensure mutex is always initialised
PCI : cpqphp : Fix possible null pointer dereference
llc : better deal with too small mtu
KVM : x86 : fix NULL deref in vcpu_scan_ioapic
perf tools : Fix segfault when using srcline sort key
igb : Fix Null - pointer dereference in igb_reset_q_vector
tracing / filters : fix off - by - one bug
[ media ] Don ' t OOPS if videobuf_dvb_get_frontend return NULL
staging : octeon : Fix warning of prefer ether_addr_copy .
sfc : insert ARFS filters with replace_equal = true
igb : fix vf lookup
[ media ] vivi : set device_caps
ASoC : Return correct codec clock in s3c64xx - i2s
dlm : close othercons
[ media ] saa7164 : better handle error codes
i40iw : Add NULL check for ibqp event handler
[ media ] gspca : reset image_len to 0 on LAST_PACKET when discarding frame
crypto : ixp4xx - don ' t leak pointers to authenc keys
ARM : EXYNOS : fix GIC using for EXYNOS5440
ALSA : timer - Fix Oops at closing slave timer
drm / i915 / gvt : add enable_execlists check before enable gvt
greybus : module : name routines consistently
bridge : netfilter : fix information leak
crypto : algif_aead - fix reference counting of null skcipher
staging : comedi : usbduxsigma : remove tidy_up () from (* auto_attach )
UDF : Close small mem leak in udf_find_entry ()
arm64 : Fix building error with 16KB pages and 36 - bit VA
ACPI : Remove acpi_pci_slot_init () headers from internal . h
ACPI : add AC / DC notifier
ASoC : Fix register cache initialisation for WM8753
e100 : make sure vlan support isn ' t advertised on old adapters
fs / 9p : destroy fid on failed remove
mac80211 : Fix frame injection using non - AP vif
memcg : fix wrong check of noswap with softlimit
drm / amd / display : Set irq state only on existing crtcs
drm / i915 : use DPLL_DVO_HIGH_SPEED for PCH eDP
dmaengine : device must have at least one channel
tools : iio : generic_buffer : free dev_dir_name on exit
mac80211 : fix rate control initialisation
bnx2x : fix possible panic under memory stress
asus - laptop : fix a memory leak in asus_laptop_get_info error path
Input : synaptics - add manual min / max quirk for ThinkPad X240
USB : ehci : add call of free_cached_itd_list () function in disable_periodic ()
drivers / atm / eni . c : ensure arguments to request_irq and free_irq are compatible
drm / i915 : Clear intel_crtc -> atomic before updating it .
nvmet - fc : ensure target queue id within range .
ASoC : wm_adsp : Fix support for firmware switching on DSP2 and higher
cifs : Silence uninitialized variable warning
kvm : x86 : make function static to avoid compiling warning
mm : vma_adjust : remove superfluous check for next not NULL
imx - drm : imx - ldb : Check ' mode ' pointer first
[ PATCH ] x86_64 : Don ' t confuse apic =... command line option with apic
leds : Use kcalloc instead of kzalloc to allocate array
ARC : fix build warnings with ! CONFIG_KPROBES
iwlwifi : mvm : reduce active dwell time
drm / i915 : A hotfix for making aliasing PPGTT work for GVT - g
[ SCSI ] ipr : Always initiate hard reset in kdump kernel
perf / x86 / intel / uncore : Correct fixed counter index check in generic code
mm : check that we have the right vma in __access_remote_vm ()
ARM : davinci : da830 - evm : specify the chipselect in davinci_nand_pdata
dmaengine : sun6i - dma : Only calculate residue if state exists .
Btrfs : don ' t run __tree_mod_log_free_eb on leaves
seq_file : disallow extremely large seq buffer allocations
lightnvm : fail fast on passthrough commands
btrfs : Check name_len before reading btrfs_get_name
staging : wilc1000 : linux_wlan . c : add kzalloc error check
ath9k : release allocated buffer if timed out
acornfb : Dont BUG () on invalid pan parameters
[ IPV6 ]: Address autoconfiguration does not work after device down / up cycle
Input : don ' t modify the id of ioctl - provided ff effect on upload failure
mailbox : add MODULE_LICENSE () for mtk - cmdq - mailbox . c
fbcon : when font is freed , clear also vc_font . data
video : wm8505fb : Remove redundant of_match_ptr
scsi : qedi : Fix a potential buffer overflow
drm / mediatek : don ' t use drm_put_dev
fs : Add a missing permission check to do_umount
crypto : ccp - Release all allocated memory if sha type is invalid
drm / i915 : release mutex in i915_gem_init ()' s error path
orinoco : fix TKIP countermeasure behaviour
staging : lustre : fix sparse warning about different address spaces
pinctrl : imx1 : fix return value check in imx1_pinctrl_core_probe ()
drm / i915 / bxt , glk : Fix assert on conditions for DC9 enabling
tile : array underflow in setup_maxnodemem ()
tracing / filter : Add missing initialization
drm : clean dirty memory after device release
sysfs : Make poll behaviour consistent
staging : dgap : Add check for MAXBOARDS in . probe ( dgap_init_one )
drivers / perf : arm_pmu_acpi : Release memory obtained by kasprintf
ALSA : timer : Fix leak in SNDRV_TIMER_IOCTL_PARAMS
GFS2 : Fix freeze issue
sch_sfb : Fix missing NULL check
ASoC : Intel : Skylake : check manifest size
ASoC : s6000 : Use WARN_ON () instead of BUG_ON ()
[ media ] drivers / media / dvb - frontends / stv0900_sw . c : Fix break placement
ath9k_hw : fix synth delay for half / quarter channels
apparmor : fix update the mtime of the profile file on replacement
drm / i915 : fix up the relocate_entry refactoring
[ PATCH ] x86 - 64 : Fix C3 timer test
perf trace / scripting : Check return val of perl_run ()
qlcnic : Fix function return error check
atm : fix info leak via getsockname ()
[ PATCH ] Bluetooth : fix potential NULL ptr deref in dtl1_cs . c :: dtl1_hci_send_frame ()
mtd : nand : mpc5121_nfc : show parent device in sysfs
sched : Remove unlikely () from ttwu_post_activation
drivers / staging / cx25821 / cx25821 - video . c : introduce missing kfree
[ S390 ] add atomic64_xchg to s390
mfd : sta2x11 - mfd : Only add sta2x11_mfd if it hasn ' t already been added
staging : vt6656 : check ieee80211_bss_conf bssid not NULL
tcp : Set CHECKSUM_UNNECESSARY in tcp_init_nondata_skb
ethtool : EEPROM dump no longer works for tg3 and natsemi
[ media ] vb2 : fix compiler warning
debugfs : set no_llseek in DEFINE_DEBUGFS_ATTRIBUTE
ip_tunnel : clear IPCB in ip_tunnel_xmit () in case dst_link_failure () is called
Btrfs : Fix test_range_bit for whole file extents
[ NETFILTER ]: nfnetlink_log : fix NULL pointer dereference
evm : checking if removexattr is not a NULL
ceph : fix erroneous cap flush to non - auth mds
[ PARISC ] set memory ranges in N_NORMAL_MEMORY when onlined
goldfish_pipe : use GFP_ATOMIC under spin lock
ASoC : Intel : Skylake : Initialize module list for Broxton
kvm : x86 : fix kvm_apic_has_events to check for NULL pointer
UWB : Return UWB_RSV_ALLOC_NOT_FOUND rather than crashing on NULL dereference if kzalloc fails
ACPI : ioapic : Clear on - stack resource before using it
crypto : atmel - aes - add new version
[ PATCH ] HID read busywait fix
brcmfmac : Add check for short event packets
xfs : validate acl count
lib : rhashtable : Correct self - assignment in rhashtable . c
ASoC : Intel : Skylake : Fix the module state check condition
ARM : S5P64X0 : Replace irq_gc_ack () with irq_gc_ack_set_bit ()
[ media ] c8sectpfe : Fix broken circular buffer wp management
mmc : core : Add card_busy to host_ops
Btrfs : fix race between readahead and device replace / removal
orinoco : Maintain lock until entry removed from list
iio : gyro : ADXRS450 : Add missing read support for calibbias attribute
batman - adv : Print jiffies as unsigned long
tipc : fix an infoleak in tipc_nl_compat_link_dump
KEYS : fix out - of - bounds read during ASN . 1 parsing
[ SCSI ] remove unnecessary scsi_delete_timer () call in scsi_reset_provider ()
staging / xgifb : Fix return of uninitialized variable
iio : adc : set INPUT_PROP_DIRECT
cifs : potential missing check for posix_lock_file_wait
scsi : fnic : Zero io_cmpl_skip on fw reset completion
rpmsg : virtio : Fix possible double free in rpmsg_virtio_add_ctrl_dev ()
ieee1394 : node manager causes up to ~ 3 . 25s delay in freezing tasks
dm crypt : fix free of bad values after tfm allocation failure
ixgbe : Fix FCOE memory leak for DDP packets
[ agp ]: fixup chipset flush for new Intel G4x .
fuse : fix SEEK_END incorrectness
mm : numa : Do not account for a hinting fault if we raced
clk : versatile : fix memory leak
drm / radeon / kms : Don ' t kzalloc memory which is immediately overwritten .
scsi : scsi_transport_iscsi : fix the issue that iscsi_if_rx doesn ' t parse nlmsg properly
mv643xx_eth : check for valid hw address ( resubmit )
pinctrl : imx : off by one in imx_pinconf_group_dbg_show ()
f2fs : skip writing in -> writepages when no dirty pages exist
iwlwifi : mvm : comply with fw_restart mod param on suspend
btrfs : fix missing last - entry in readdir ( 3 )
mmc : sdhci - st : explicitly request exclusive reset control
ASoC : Hold client_mutex while calling snd_soc_instantiate_cards ()
kconfig / nconf : prevent segfault on empty menu
HID : logitech : fix bounds checking on LED report size
drivers / net / pcmcia / 3c589_cs : fix port configuration switcheroo
sc16is7xx : enable the clock
UDF : Fix a null pointer dereference in udf_sb_free_partitions
drm / bridge : synopsys : dw - hdmi : clean up drm_bridge_add call
x86 : x86 CORE_DUMP_USE_REGSET
fs : logfs : Fix up MTD = y build .
arm / arm64 : KVM : Load the timer state when enabling the timer
hfsplus : fix Buffer overflow with a corrupted image
[ PATCH ] aacraid : swapped kmalloc args .
spi : zynq : missing break statement
isicom : Deinline drop_dtr , save 112 bytes
ath9k_htc : Fix free slot value for cab queue
tick / nohz : Fix possible missing clock reprog after tick soft restart
[ media ] pvrusb2 : don ' t go past buf array
Btrfs : fix memory leak in btrfs_ioctl_start_sync ()
Btrfs : Don ' t error on resizing FS to same size
PCI : tegra : Fix host bridge memory leakage
drm : stop information leak of old kernel stack .
perf / core : Fix the perf_cpu_time_max_percent check
KVM : PPC : Fix oops when checking KVM_CAP_PPC_HTM
[ IPV4 ] fib_hash : fix duplicated route issue
drm / atmel - hlcdc : check stride values in the first plane
[ media ] davinci : vpbe : fix build error when CONFIG_VIDEO_ADV_DEBUG is enabled
wcn36xx : Fix error handling
stmmac : Remove unbalanced clk_disable call
mmc : mmci : Cascade EPROBE_DEFER from regulators .
arch / tile : check kmalloc () result
staging : wilc1000 : fix parameter name of function declaration
HID : hidraw : Fix crash on HIDIOCGFEATURE with a destroyed device
[ GFS2 ] gfs2 endianness bug : be16 assigned to be32 field
mfd : arizona : Check errors from regcache_sync ()
btrfs : check file extent backref offset underflow
dmaengine : Stricter legacy checking in dma_request_slave_channel_compat ()
rtlwifi : fix memory leak for USB device
HID : hid - picolcd : Fix memory leak in picolcd_debug_out_report ()
ath6kl : Support net_stats . multicast
netrom : fix invalid use of sizeof in nr_recvmsg ()
IPoIB : Fix crash in path_rec_completion ()
linux / types . h : Use __u64 for aligned_u64
slub : Zero initial memory segment for kmem_cache and kmem_cache_node
ipmi : info leak in compat_ipmi_ioctl ()
bpf : fix double - fdput in replace_map_fd_with_map_ptr ()
tcm_loop : Fix memory leak in tcm_loop_submission_work error path
spectra : Don ' t overwrite nand_debug_level . It ' s a module param
gpio : GPIO_GET_CHIPINFO_IOCTL : Fix information leak
fib_semantics : Don ' t match route with mismatching tclassid
scsi : ufs : ufs - mediatek : Fix error checking in ufs_mtk_init_va09_pwr_ctrl ()
sunrpc : Avoid a KASAN slab - out - of - bounds bug in xdr_set_page_base ()
drm / edid : Don ' t skip every eighth EST III mode
GFS2 : O_TRUNC not working on stuffed files across cluster
platform / x86 : intel_ips : Use PCI_VDEVICE () macro
powerpc / cell : Fix integer constant warning
rxrpc : Make service connection lookup always check for retry
DLM : fix overflow dlm_cb_seq
tty : Fix GPF in flush_to_ldisc ()
ax25 : fix NPD bug in ax25_disconnect
inet : properly align icsk_ca_priv
perf / core : Fix perf_uprobe_init ()
crypto : md5 - Set statesize
drm / atomic : Handle funcs -> best_encoder == NULL case
include / linux / poison . h : fix LIST_POISON { 1 , 2 } offset
[ XFS ] Fix nused counter . It ' s currently getting set to - 1 rather than
[ SCSI ] ibmvscsi : Add 16 byte CDB support
perf inject : Do not repipe attributes to a perf . data file
ath5k : free sc -> ah in ath5k_deinit_softc
mac80211 : read station mgmt keys via get_key call
i40e : keep service tasks out of reset process
fs / select . c : fix information leak to userspace
powerpc / perf : Fix crash in perf_instruction_pointer () when ppmu is not set
tty : Fix LED error return
ovl : Return - ENOMEM if an allocation fails ovl_lookup ()
[ MIPS ] Fix oprofile module unloading
btrfs : fix NULL pointer dereference when deleting device by invalid id
pch_dma : fix error return code in pch_dma_probe ()
ocfs2 : free path in ocfs2_remove_inode_range ()
drm / amd / display : Correct fixed point calculation .
module : don ' t call percpu_modfree on NULL pointer .
PCI : xilinx : Dispose of MSI virtual IRQ
scsi : sg : reset ' res_in_use ' after unlinking reserved array
clk : make sure parent is not NULL in clk_fetch_parent_index ()
ASoC : imx - ssi : add check on platform_get_irq return value
perf evlist : Return first evsel for non - sample event on old kernel
ASoC : topology : Add support for compressed PCMs
drm / i915 : Clear the pending_gpu_fenced_access flag at the start of execbuffer
spi : qup : Fix return value checking for pm_runtime_get_sync ()
coresight : fix kernel panic caused by invalid CPU
Staging : speakup : free after input_unregister_device ()
staging : fsl - dpaa2 / eth : Check if notification rearm is successful
staging : comedi : das16m1 : check for subdev_8255_init () failure
usb : gadget : f_ecm : fix missing unlock on error in ecm_alloc ()
wl12xx_sdio : set interrupt as wake_up interrupt
drbd : abort drbd_start_resync if there is no connection
usb : gadget : mv_udc : fix readl error
bpf : fix missing error return in check_stack_boundary ()
mfd : db8500 : Update cpufreq device name
[ PATCH ] drivers / message / i2o / pci . c : fix a NULL pointer dereference
memcg : fix mem_cgroup_out_of_memory () return value .
crypto : dcp - Check the return value from devm_ioremap_resource ()
mei : start disconnect request timer consistently
firewire : ohci : correct signedness of a local variable
VSOCK : Fix missing msg_namelen update in vsock_stream_recvmsg ()
drm / amd / display : fix gamma setting
RDMA : Fix goto target to release the allocated memory
Staging : wilc1000 : Prefer eth_broadcast_addr over memset ()
stmmac : fix noderef . cocci warnings
shm : optimize exit_shm ()
Fix a dumb typo - use of & instead of &&
drm : missing drm_vm . c changes for consistent maps
drm / i915 : Cancel pending execlists irq handler upon idling
dmaengine : ste_dma40 : off by one in d40_of_probe ()
msm : timer : fix missing return value
drm / i915 : Initialize return value for empty i915_gem_object_unbind ()
batman - adv : remove obsolete skb_reset_mac_header () in batadv_bla_tx ()
powerpc / perf : Cap 64bit userspace backtraces to PERF_MAX_STACK_DEPTH
HID : Add mapping of new KEY_MEDIA_REPEAT keycode
ixgbe : use kzalloc for allocating one thing
rds : fix an infoleak in rds_inc_info_copy
[ media ] dvb : si21xx : buffer overflow in si21_writeregs ()
RDS : verify the underlying transport exists before creating a connection
x86 : ptrace , bts : fix an unreachable statement
wanxl : fix info leak in ioctl
isdn : avoid copying too long drvid
perf / x86 : Correctly use FEATURE_PDCM
[ MIPS ] klconfig add missing bracket
i2c : ismt : prevent memory corruption in ismt_access ()
genirq : Fix type inconsistency
[ PATCH ] fuse : handle error INIT reply
i7core_edac : do not export static functions
[ PATCH ] USB : check for device in zd1201_resume
mtd : spi - nor : Off by one in cqspi_setup_flash ()
ALSA : hda - Skip card registration when no codec is found
drivers / net / fs_enet : remove null pointer dereference
[ PATCH ] ISDN : Avoid a potential NULL ptr deref in ippp
PCI : pciehp : disable DLL state changed event notification
mfd : Fix more undefined twl4030 - power resconfig value checks
[ PATCH ] knfsd : Protect update to sn_nrthreads with lock_kernel
Staging : vt6656 : checkpatch : fix identifier name in wcmd . h
wimax : i2400 : Fix memory leak in i2400m_op_rfkill_sw_toggle
iscsi - target : missing kfree () on error path
reiserfs : Fix softlockup while waiting on an inode
mac80211 : Clear wowlan flag when drv_suspend returns failure
[ media ] solo6x10 : Fix solo_eeprom_read retval type
bpf : devmap fix arithmetic overflow in bitmap_size calculation
ASoC : sgtl5000 : defer the probe if clock is not found
simple_xattr : permit 0 - size extended attributes
btrfs : add check to sysfs handler of label
Input : wm97xx - core - use IRQF_SAMPLE_RANDOM
USB : io_ti : Fix NULL dereference in chase_port ()
sched : suppress RCU lockdep splat in task_fork_fair
[ media ] ov9650 : off by one in ov965x_enum_frame_sizes ()
drm / i915 : Avoid NULL ctx -> obj dereference in debugfs / i915_context_info
UBIFS : replace count * size kzalloc by kcalloc
cxlflash : off by one bug in cxlflash_show_port_status ()
qla2xxx : Update entry type 270 to match spec update .
crypto : shash - Fix unaligned calculation with short length
KEYS : potential uninitialized variable
UBI : Silence an unintialized variable warning
arm / arm64 : KVM : Don ' t allow creating VCPUs after vgic_initialized
video : uvesafb : Fix integer overflow in allocation
drm / i915 / userptr : reject zero user_size
lockdep : fix overflow in the hlock shrinkage code
xfrm : Restrict extended sequence numbers to esp
ipx : call ipxitf_put () in ioctl error path
dccp : do not send reset to already closed sockets
pinctrl / amd : Drop pinctrl_unregister for devm_ registered device
rds : memory leak in __rds_conn_create ()
timekeeping : Make local variables static
mtd : onenand : omap2 : Remove skip initial unlocking support
UBI : always re - read in case of read failures
usb : chipidea : Fix memleak for ci -> hw_bank . regmap when removal
staging : comedi : unionxx5 : fix memory leak during subdevice init
staging : comedi : cb_das16_cs : return ' 0 ' for successful attach
farsync : fix info leak in ioctl
tg3 : Update version to 3 . 109
vfs : rename : check backing inode being equal
drm / i915 : Discard objects from mm global_list after being shrunk
tty : release_one_tty () forgets to put pids
ALSA : HDA : Fix internal microphone on Dell Studio 16 XPS 1645
Staging : sb105x : info leak in mp_get_count ()
staging : usbip : vhci_hcd : fixed suspend - resume loop
vfs : extend vfs_removexattr locking
misc : mic : Fix for double fetch security bug in VOP driver
greybus : audio_codec : Free gccodec on codec probe failure
video : goldfishfb : fix memory leak on driver remove
TTY : drop driver reference in tty_open fail path
scsi : bfa : release allocated memory in case of error
drbd : Becoming sync target may not happen out of < C_WF_REPORT_PARAMS
writeback : avoid unnecessary determine_dirtyable_memory call
drm / i915 : enable HDMI mode on VLV when an HDMI sink is detected
watchdog : davinci : Handle return value of clk_prepare_enable
[ POWERPC ] arch / powerpc / kernel / vio . c of_node_put cleanup
wil6210 : Improve AP stop handling
mwifiex : using right aid value for tdls action frame
[ POWERPC ] EEH : hotplug recovery bugfix
mac80211 : fix ieee80211_sta_process_chanswitch for 5 / 10 MHz channels
pci : Don ' t crash when reading mpss from root complex
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
ath9k_hw : reduce delay on programming INI on AR9003
vfs : mnt_want_write_file (): fix special file handling
Btrfs : only do the tree_mod_log_free_eb if this is our last ref
ipv4 : check optlen for IP_MULTICAST_IF option
HID : enable hid device to suspend / resume asynchronously
kernel / signal . c : stop info leak via the tkill and the tgkill syscalls
libnvdimm , pmem : fix persistence warning
[ media ] pulse8 - cec : set correct Signal Free Time
ksmbd : fix use - after - free bug in smb2_tree_disconect
cxgb4 : Inform caller if driver didn ' t upgrade firmware
[ S390 ] cio : path_event overindication after resume
[ PATCH ] e1000 : Enable polling before enabling interrupts
[ PATCH ] uml : switch_mm fix
sfc : don ' t BUG_ON efx -> max_channels == 0 in probe
drm / amdgpu_gem : fix error handling path in amdgpu_gem_va_update_vm
netrom : fix info leak via msg_name in nr_recvmsg ()
ALSA : usb - audio : add DSD support for new Amanero PID
USB : visor : fix null - deref at probe
TG3 : limit reaches - 1
[ media ] media - device : fix infoleak in ioctl media_enum_entities ()
[ media ] easycap : Fix mem leak in easycap_usb_probe ()
tools : ffs - aio - example : add missing wMaxPacketSize for HS descs
ide : fix LOCKDEP warning
brcmfmac : Fix crash on cleanup .
dmaengine : imx - sdma : fix up param for the last BD in sdma_prep_slave_sg ()
ubi : Fix whitespace issue in count_fastmap_pebs ()
Btrfs : skip commit transaction if we don ' t have enough pinned bytes
drm / i915 : Missed lock change with rps lock
USB : fix dereference in drivers / usb / misc / adutux . c
ipmi : fasync BKL pushdown
USB : core : harden cdc_parse_cdc_header
clk : tegra : cclk_lp has a pllx / 2 divider
iio : st_pressure : Export sampling frequency for lps25h and lps331ap
rpmsg : glink : The mbox client knows_txdone
sfc : initialize dynamic sysfs attributes for lockdep
mtd : mxc_nand : Propagate the error if platform_get_irq () fails
bonding : disable netpoll on enslave failure
i40e : Fix memory leak at failure path in i40e_dbg_command_write ()
x86 , e820 : Guard against array overflowed in __e820_add_region ()
svcrdma : Add put of connection ESTABLISHED reference in rdma_cma_handler
ath9k : Do not use local_bh_disable in ampdu_action
virtio_net : enable big packets for large MTU values
blkcg : fix double free of new_blkg in blkcg_init_queue
ext4 : fix deadlock with quota feature
drm / msm / hdmi : redefinitions of macros not required
[ ARM ] fix leak in iop13xx / pci
cyclades : fix read buffer overflow
[ DLM ] Fix memory leak in dlm_add_member () when dlm_node_weight () returns less than zero
vesafb : fix memory leak
iommu / vt - d : Fix a potential memory leak
USB : cdc - acm tell tty layer not to split things up .
KVM : arm / arm64 : vgic : Fix kvm_vgic_map_is_active ' s dist check
KVM : emulate : avoid accessing NULL ctxt -> memopp
Staging : rtl8188eu : usb_halinit : Use ! x instead of x == NULL .
ext4 : check for zero length extent
[ TCP ]: Fix TCP header misalignment
mtd : lpc32xx_slc : Make probe () return - EPROBE_DEFER if necessary
USB : serial : console : fix use - after - free on disconnect
ASoC : Intel : Skylake : fix noderef . cocci warnings
fscrypt : fix renaming and linking special files
staging : fbtft : Fix potential null dereference
[ SCTP ]: Fix kernel panic while received AUTH chunk while enabled auth
be2net : fix pmac_id for BE3 VFs
s390 / qeth : use QDIO_ * _QFMT defines
perf sched : Look up thread using tid instead of pid
greybus : hd : fix cport - count check
phonet : refcount leak in pep_sock_accep
sunrpc : integer underflow in rsc_parse ()
s390 / cpumf : add missing lpp magic initialization
x86 / mm : Fix use - after - free of ldt_struct
iwlwifi : dvm : disable power save by default
selinux : fix kernel BUG on prlimit (..., NULL , NULL )
libceph : delete from need_resend_linger before check_linger_pool_dne ()
[ XFS ] fix valid but harmless sparse warning
drm / nouveau / devinit / nv04 : change owner to int
rpmsg : virtio : Fix possible double free in rpmsg_probe ()
cfg80211 : Clear " beacon_found " on regulatory restore
x86 / apic / irq_remap : Silence a bogus pr_err ()
drm / i915 : Change WARN to ERROR in CSB count
kcm : fix a null pointer dereference in kcm_sendmsg ()
[ PATCH ] ia64 : add ptr_to_compat ()
drm / nouveau / bios / gp104 : initial support
greybus : svc : Pass the correct pointer to input_free_device ()
[ media ] ttusb - dec : buffer overflow in ioctl
xfs : fix quotacheck dquot id overflow infinite loop
ipmi : fix use - after - free of user -> release_barrier . rda
iwlwifi : reduce max remain - on - channel duration
drm / msm / mdp5 : make helper function static
[ PATCH ] oom : kthread infinite loop fix
USB : cdc - acm : more sanity checking
iommu / amd : Fix BUG when faulting a PROT_NONE VMA
ASoC : Fix incorrect kfree in wm8731_probe error path
ASoC : Fix reg_word_size for ak4104
ext4 : do not use stripe_width if it is not set
rtc : ds1374 : Set . of_match_table to OF device ID table
iio : core : Handle error when mask type is not separate
drm / i915 : Fix CSR MMIO address check
nvmet - rdma : occasionally flush ongoing controller teardown
ipv6 - multicast : Fix memory leak in IPv6 multicast .
clk : hisilicon : Use kcalloc () in hisi_clk_init ()
USB : serial : console : fix use - after - free after failed setup
crypto : kpp - add get / set_flags helpers
[ XFS ] Fix realtime subvolume expansion , a porting bug b0rked it . Coverity
atm : ambassador : use NULL instead of 0 for pointer
pty : fix data loss when stopped (^ S /^ Q )
staging : ks7010 : change local variable type in ks_wlan_hw_rx
s390 / ipl : always use load normal for CCW - type re - IPL
drbd : fix possible access after free
staging : unisys : delete extra space character in vbuschannel_itoa ()
ext4 : fix potential integer overflow
ipv6 : ipv6_find_hdr restore prev functionality
ASoC : Instantiate card widgets immediately
thinkpad_acpi : Fix a memory leak during module exit
fs : clear writeback errors in inode_init_always
KVM : PIT : control word is write - only
mac80211 : reset CQM history upon reconfiguration
NFS : Clean up : revert increase in READDIR RPC buffer max size
drivers / net / wireless / b43 / main . c : 4351 : Fixed coding style
HID : hidraw , fix a NULL pointer dereference in hidraw_ioctl
mmc : sdhci - spear : Handle return value of platform_get_irq
KVM : PPC : Book3S : Check KVM_CREATE_SPAPR_TCE_64 parameters
mac80211 : fix conn_mon_timer running after disassociate
kernel / relay . c : fix potential memory leak
ASoC : tlv320aic3x : Don ' t sync first two registers from register cache
ALSA : control : Make sure that id -> index does not overflow
ibmvnic : fix missing unlock on error in __ibmvnic_reset ()
bdi : register sysfs bdi device only once per queue
stmmac : add missing of_node_put
dmaengine : pl330 : do not emit loop for 1 byte transfer .
ARM : omap - device : add missed callback for suspend - to - disk
ata_piix : add short 40c quirk for Acer Aspire 2030 , take # 2
tg3 : Fix 5906 link problems
malidp : Fix NULL vs IS_ERR () checking
usb : mon : make mmapped memory read only
ALSA : hda - Fix auto - mic detection in Conexant codec - parser
ext4 : verify extent header depth
ext4 : Eliminate potential double free on error path
ieee1394 : SPIN_LOCK_UNLOCKED cleanup
imx - drm : imx - drm - core : improve safety of imx_drm_add_crtc ()
ceph : fix cap_wanted_delay_ { min , max } mount option initialization
arm64 : make sys_call_table const
rtl8xxxu : prevent leaking urb
staging / dgnc : fix info leak in ioctl
tcp : fix TSO FACK loss marking in tcp_mark_head_lost
[ ALSA ] sound / isa / gus / interwave . c : check kmalloc () return value
UBI : mtd / ubi / vtbl . c : fix memory leak
batman - adv : randomize initial seqno to avoid collision
[ IPV6 ] ROUTE : Prefer reachable nexthop only if the caller requests .
dmaengine : zynqmp_dma : Fix race condition in the probe
Btrfs : fix extent_same allowing destination offset beyond i_size
rpmsg : Only invoke announce_create for rpdev with endpoints
e1000 : check on netif_running () before calling e1000_up ()
ipmi : Fix memory leak in __ipmi_bmc_register
staging : iio : lpc32xx_adc : Fix IRQ check
aacraid : missing capable () check in compat ioctl
i40e : Setting queue count to 1 using ethtool is valid
ARM : EXYNOS : Fix potential NULL pointer access in exynos_sys_powerdown_conf
mm : fix negative nr_isolated counts
[ PATCH ] kyrofb : Fix uninitialized value
locks : fix list insertion when lock is split in two
ext4 : Fix overflow caused by missing cast in ext4_fallocate ()
staging : omapdrm / omap_gem_dmabuf . c : fix memory leakage
aha1542 : Convert aha1542_intr_reset to function
cifs : fix potential overflow in cifs_compose_mount_options
ath9k : do not register LEDs on AR913x
i2c : ocores : Fix pointer to integer cast warning
USB : ohci_usb can oops on shutdown
udp : fix if statement in SIOCINQ ioctl
[ MTD ] [ NAND ] Casting bug in nand_default_block_markbad
[ POWERPC ] kprobes : Fix possible system crash during out - of - line single - stepping
iio : adc : meson - saradc : unlock on error in meson_sar_adc_lock ()
keys : fix race with concurrent install_user_keyrings ()
[ TCP ]: Avoid a divide in tcp_mtu_probing ()
HID : emsff : properly handle emsff_init failure
isofs : avoid info leak on export
ext4 : fix memory leak in xattr
mwifiex : pcie : Fix memory leak in mwifiex_pcie_init_evt_ring
mfd : Mark two lm3533 zone registers as volatile
cgroup : fix a leak when percpu_ref_init () fails
tun : handle register_netdevice () failures properly
gpio : mb86s70 : Return error if requesting an already assigned gpio
Btrfs : Check for NULL page in extent_range_uptodate
x86 / fpu / xstate : Return NULL for disabled xstate component address
[ PATCH ] kprobes / ia64 : refuse inserting kprobe on slot 1
ceph : avoid divide by zero in __validate_layout ()
xen : make xen_flush_tlb_all () static
UBI : fix error path in __wl_get_peb
ARM : OMAP2 : Fix tusb6010 init error and compilation warning
iwlwifi : don ' t ack the card state notification
ath10k : fix memory leak
ks8851 : Fix request_irq / free_irq mismatch
TOMOYO : Fix incomplete read after seek .
f2fs : wake up all waiters in f2fs_submit_discard_endio
USB : wusb : check CHID is all zeros before stopping the host
perf annotate browser : Exit when pressing ESC or the left arrow
ring - buffer : check for less than two in size allocation
ibmvnic : Sanitize entire SCRQ buffer on reset
drm / exynos : fix memory leak to EDID block
usb : hub : Don ' t wait for connect state at resume for powered - off ports
qlcnic : Dump mailbox command data when a command times out
fcoe : make sure fcoe frames are unshared prior to manipulating them
tcp : fix zero cwnd in tcp_cwnd_reduction
rtlwifi : rtl8188ee : Fix potential race condition
KVM : Fix pit memory leak if unable to allocate irq source id
l2tp : fix info leak in l2tp_ip6_recvmsg ()
[ PATCH ] off - by - 1 in kernel / power / main . c
[ media ] soc_camera : ret is never used . get rid of it
md : bcache : io . c : fix a potential NULL pointer dereference
sunrpc : don ' t register UDP port with rpcbind when version needs congestion control
ARM : davinci : neuros - osd2 : specify the chipselect in davinci_nand_pdata
Btrfs : send correct rdev and mode in btrfs - send
[ SCSI ] iscsi_tcp : update header size during relogin
max17042_battery : Handle irq request failure case
iio : chemical : ccs811 : Corrected firmware boot / application mode transition
USB : devio : fix issue with log flooding
staging : comedi : ni_660x : remove BUG_ON () in ni_660x_request_mite_channel ()
dmaengine : at_hdmac : use tasklet_kill in teardown
tg3 : Disable 5717 serdes and B0 support
batman - adv : Add BATADV_DBG_TP_METER to BATADV_DBG_ALL
thermal : rockchip : optimize sensor auto accessing period
ipv6 : ignore null_entry in inet6_rtm_getroute () too
[ media ] cx231xx : prints error code if can ' t switch TV mode
rtlwifi : rtl8192se : Synchronize IRQ after disabling it
wifi : wilc1000 : validate length of IEEE80211_P2P_ATTR_CHANNEL_LIST attribute
fuse : don ' t wake up reserved req in fuse_conn_kill ()
offb : the pseudo_palette is only 16 elements long
[ S390 ] cio : Fix ungroup vs . machine check race in ccwgroup .
ext3 : fdatasync should skip metadata writeout when overwriting
drm / amdgpu : update rev id for hainan
xfs : check for a valid error_tag in errortag_add
[ PATCH ] USB : Resource leak fix for whiteheat driver
ALSA : timer : Fix leak in events via snd_timer_user_ccallback
[ MIPS ] Sibyte : Fix name of clocksource .
powerpc / xive : Fix IPI reset
bcma : support chipsets with PMU and GCI cores ( devices )
KVM : fix LAPIC timer period overflow
gpio : omap : add missed spin_unlock_irqrestore in omap_gpio_irq_type
jfs : prevent NULL deref in diFree
memcg : fix leak of accounting at failure path of hugepage collapsing
iscsi - target : fix extract_param to handle buffer length corner case
cpufreq : Avoid calling cpufreq driver ' s target () routine if target_freq == policy -> cur
[ PATCH ] libata : fast exit from EH while unloading
[ PATCH ] Char : mxser_new , correct include file
pppoe : Lacks DST MAC address check
xfs : fix acl count validation in xfs_acl_from_disk ()
usb - uas : set max_lun and max_channel
mips : handle pgtable_page_ctor () fail
dlm : avoid unnecessary search in search_rsb
ASoC : blackfin : Fix missing break
mac80211 : fix statistics leak if dev_alloc_name () fails
usb : gadget : atmel_usba_udc : Remove unneeded condition
xfs : fix off - by - one error in xfs_attr3_rmt_verify
md : use kzalloc () when bitmap is disabled
console / vgacon . c : mark file - local symbol static
[ media ] bfin_capture : fix compiler warning
blkcg : don ' t create " io . stat " on the root cgroup
hwmon : ( stts751 ) buffer overrun on wrong chip configuration
i7core_edac : don ' t free on success
hwmon : ( adm1025 ) Fix vrm write operation
blkcg : stop iteration early if root_rl is the only request list
tg3 : Restrict ASPM workaround devlist
perf thread_map : Correctly size buffer used with dirent -> dt_name
Staging : comedi : off by one in doDevConfig ()
tun : allow positive return values on dev_get_valid_name () call
module : fix memory leak on early load_module () failures
staging : mt7621 - dma : Fix ident by space
libbpf : fix string comparison for guessing eBPF program type
selinux : fix error return code in policydb_read ()
cgroup : add sparse annotation to cgroup_iter_start () and cgroup_iter_end ()
ARM : mvebu : mvebu - soc - id : add missing clk_put () call
uprobes : split THPs before trying to replace them
HSI : nokia - modem : Use devm_kcalloc () in nokia_modem_gpio_probe ()
ip_gre : Set needed_headroom dynamically again
mac80211 : minstrel : avoid port control frames for sampling
drm / i915 : don ' t pwrite tiled objects through the gtt
ASoC : wm5102 : Correct lookup of arizona struct in SYSCLK event
xfrm : fix possible null deref in xfrm_init_tempstate
drm / i915 : check the power domains in ironlake_get_pipe_config ()
[ ALSA ] sound / pci / cs46xx /: fix an off - by - one
ext4 : remove unnecessary ext4_get_group_info in ext4_mb_load_buddy
rt2x00 : Use IEEE80211_TX_CTL_STBC flag
crypto : qat - silence a static checker warning
mac80211 : Apply ht_opmode changes in ieee80211_change_bss
PCI : cpqhp : register busn_res
tty : amba - pl011 : don ' t dereference NULL platform data
cpufreq_stats : do not remove sysfs files if frequency table is not present
i40e : Expose ' trust ' flag to userspace via ndo_get_vf_config .
iio : adc : xilinx - xadc : assign auxiliary channels address correctly
dma : struct device - replace bus_id with dev_name (), dev_set_name ()
ipv6 : fix a typo in fib6_rule_lookup ()
af_key : initialize satype in key_notify_policy_flush ()
kernel / exit . c : call proc_exit_connector () after exit_state is set
drivers / video / backlight / lcd . c : call put_device if device_register fails
block : fix use - after - free in seq file
[ TCP ]: Do receiver - side SWS avoidance for rcvbuf < MSS .
sctp : fix possible seqlock seadlock in sctp_packet_transmit ()
drm / i915 : Can ' t do accurate vblank timestamps with UMS
OMAPDSS : fix registering the vsync isr in apply
perf tools : Disallow PMU events intel_pt and intel_bts until there is support
perf ui hist browser : Fix segfault on ' a ' for annotate
liquidio : update module parameter fw_type to reflect firmware type loaded
bcache : fix use - after - free in btree_gc_coalesce ()
mnt : Fix the error check in __detach_mounts
KVM : nVMX : No need for handle_vmx_insn function any more
e1000e : e1000e_cyclecounter_read (): fix er32 ( SYSTIML ) overflow check
iommu / vt - d : Remove warning for HPET scope type
vmwgfx : return an - EFAULT if copy_to_user () fails
ipmi : fix platform return check
ACPI : Add DMI entry for Sony VGN - FW41E_H
[ ALSA ] fix resource leak in usbmixer
KVM : x86 : emulator : Return to user - mode on L1 CPL = 0 emulation failure
ASoC : WM8903 : Free IRQ on device removal
i40evf : Add additional check for reset
dccp : fix info leak via getsockopt ( DCCP_SOCKOPT_CCID_TX_INFO )
spi : lpspi : fix memory leak in fsl_lpspi_probe
cxl : remove redundant increment of hwirq
clk : shmobile : Free ' clock ' on error path
firewire : cdev : fix another memory leak in an error path
cs5535 - mfgpt : don ' t call __init function from __devinit
drm : make drm_minors_cleanup () static
ISCSI : fix minor memory leak
Btrfs : fix inode eviction infinite loop after extent_same ioctl
i40e : Fix a sleep - in - atomic bug
usb : musb : blackfin : call gpio_free () on error path in musb_platform_init ()
tracing : Uninitialized variable in create_tracing_map_fields ()
dm cache : fix leaking of deferred bio prison cells
usb : phy : r - car gen2 : use usb_add_phy_dev
bpf : Fix integer overflow in prealloc_elems_and_freelist ()
[ media ] vivid : fix broken Bayer text rendering
RDS : null pointer dereference in rds_atomic_free_op
ARM : 8305 / 1 : DMA : Fix kzalloc flags in __iommu_alloc_buffer ()
[ CPUFREQ ] Do not set policy for offline cpus
Btrfs : add missing error check to find_parent_nodes
acer - wmi : avoid the warning of ' devices ' may be used uninitialized
[ XFRM ]: Use output device disable_xfrm for forwarded packets
drm / radeon / kms : fix macbookpro connector quirk
futex_unlock_pi () hurts my brain and may cause application deadlock
dm : mark function lists static
USB : serial : io_ti : fix div - by - zero in set_termios
ASoC : Intel : Skylake : Fix memory leak during init instance
ath9k : check for NULL rate when using ieee80211_get_rts_cts_rate ()
iwl : off by one bug
[ media ] v4l2 - mem2mem : clear m2m queue ready counter in v4l2_m2m_streamoff
acer - wmi : set the touchpad toggle key code to KEY_TOUCHPAD_TOGGLE
ASoC : rt5514 : Fix uninitialized calibration value
Input : powermate - fix oops with malicious USB descriptors
Input : ALPS - fix command mode check
irda : Fix missing msg_namelen update in irda_recvmsg_dgram ()
bnx2x : Add BCM84823 to the supported PHYs
xen / pci : Check for PCI bridge before using it .
ath9k : fix channel frequency calculation for AR9340
iomap : prepare iomap_fiemap for attribute mappings
ARM : mxc : free dma_mask in error path
ptr_ring : fail early if queue occupies more than KMALLOC_MAX_SIZE
KVM : MMU : Set shadow pte atomically in mmu_pte_write_zap_pte ()
cpufreq : intel_pstate : Fix limits -> max_perf rounding error
USB : serial : visor : fix crash on detecting device without write_urbs
Input : return correct size when reading modalias attribute
eCryptfs : Copy up lower inode attrs after setting lower xattr
kbuild : fixdep : Check fstat ( 2 ) return value
stmmac : re - add IFF_UNICAST_FLT for dwmac1000
x86 / msr : Add capabilities check
scsi : qla2xxx : Fix system crash for Notify ack timeout handling
KVM : x86 : Check lapic_in_kernel () before attempting to set a SynIC irq
arm64 : Move BP hardening to check_and_switch_context
dmaengine : fix dependency chaining
drm / vmwgfx : Don ' t flush fb if we ' re in the suspended state .
USB : serial : io_ti : fix information leak in completion handler
xhci : Set URB actual length for stopped control transfers
sched : debug : update exec_clock only when SCHED_DEBUG
ALSA : emu10k1 - Fix digital / analog switch on audigy2 ZS
RDS : Heap OOB write in rds_message_alloc_sgs ()
ASoC : intel : log an error on double free
genetlink : Fix a memory leak on error path
mwifiex : fix command 0x2c timeout during p2p_find or p2p_connect
HID : elo : fix memory leak in elo_probe
mfd : db8500 - prcmu : Check return of devm_ioremap for error
sg_start_req (): make sure that there ' s not too many elements in iovec
drm / amdkfd : Handle case of invalid queue type
ARM : ux500 : set coherent_dma_mask for dma40
mtd : mtdpart : fix add_mtd_partitions error path
audit : don ' t generate audit feature changed log when audit disabled
watchdog : bcm7038 : Check the return value from clk_prepare_enable ()
ethtool : time to blink provided in seconds not jiffies
ASoC : simple - card : Add a NULL pointer check in asoc_simple_card_dai_link_of
perf probe : Load map before glob matching
staging : lustre : llog : prevent out - of - bound index
f2fs : handle error case with f2fs_bug_on
ASoC : omap - pcm : Free dma buffers in case of error .
af_key : initialize xfrm encap_oa
mac80211 : fix scan_sdata assignment
m68k : macide doesnt check for Mac
dm crypt : use memzero_explicit for on - stack buffer
gma500 : Consider CRTC initially active .
staging : ozwpan : Set farewell report length .
x86 , mce : reinitialize per cpu features on resume
vfs : ioctl : prevent double - fetch in dedupe ioctl
KEYS : Fix keyring ref leak in join_session_keyring ()
drm / i915 : switch off backlight for backlight class 0 brightness
x86 / boot : Fix minor fd leakage in tools / relocs . c
drm / helper : lock all around force mode restore
[ media ] uvcvideo : Fix race of open and suspend in error case
ASoC : simple - card : tidyup return method from probe ()
USB : PD : define specific requests
hpsa : Checking for a NULL return from a kzalloc call
memcg : hierarchy avoid unnecessary reclaim
[ NETFILTER ]: recent match : missing refcnt initialization
[ XFS ] Wait for all I / O on truncate to zero file size
greybus : svc : disable connection after all its users are gone
dmaengine : pxa_dma : declare transfer are reusable
mfd : tc6393xb : Handle return value of clk_prepare_enable
ixgbe : return error on unsupported SFP module when resetting
staging : lustre : libcfs : fix aligment issue
Btrfs : free and unlock our path before btrfs_free_and_pin_reserved_extent ()
drm / amdgpu : it should disable gfxoff when system is going to suspend
batman - adv : Calculate extra tail size based on queued fragments
staging : comedi : ni_6527 : reset edge detection registers
bq20z75 : Enable detection after registering
drm / nouveau / pmu : make sure the reset hook exists before running it
serial : 8250 : of : Defer probe on missing IRQ
ARM : mm : allow set_memory_ *() to be used on the vmalloc region
printk : use vprintk_func in vprintk ()
staging : dgnc : Use kcalloc instead of kzalloc .
hwmon : ( pc87360 ) Fix vrm write operation
staging , vt6656 / wpactl . c : Fix mem leak in wpa_ioctl ()
iwlagn : block regular host commands if driver don ' t own uCode
ksmbd : fix memory leak in smb2_handle_negotiate
[ MTD ] CORE mtdchar . c : fix off - by - one error in lseek ()
rt2x00 : allow AP and mesh mode to operate simultaneously
kernel / params . c : fix the maximum length in param_get_string
pinctrl / nomadik : use zero as default irq_start
OMAPDSS : TPO - TD03MTEA1 : Check for errors from regulator_enable ()
ath5k : temporarily disable crypto for AP mode
OMAP : HWMOD : Adding clockdomain check
regmap : cache : Don ' t attempt to sync non - writeable registers
drm / radeon : add backlight quirk for hybrid mac
s390 / dasd : fix list corruption for sleep_on requests
ipv4 : Missing sk_nulls_node_init () in ping_unhash ().
iwlwifi : mvm : Set valid TX antennas value before calib request
mt76 : fix variable gain adjustment range
tracing / filters : free pred when clearing filters
iio : imu : adis16400 : fix memory leak
ipvs : fix info leak in getsockopt ( IP_VS_SO_GET_TIMEOUT )
clk : shmobile : r8a7795 : Add USB3 . 0 clocks
ALSA : asihpi : incorrect range check
tracing : Add entry -> next_cpu to trace_ctxwake_bin ()
mm : avoid endless looping for oom killed tasks
mac80211_hwsim : fix possible memory leak in hwsim_new_radio_nl ()
x86 : add dummy acpi_numa_processor_affinity_init () implementation on 32 - bit
gpio : Propagate all errors in devm_get_gpiod_from_child ()
drm / i915 : Don ' t leave old junk in ilk active watermarks on readout
brcmfmac : Fix memory leak and missing assignment .
kvm : nVMX : Don ' t allow L2 to access the hardware CR8
mtd : nand : ifc : Initialize SRAM for all version >= 1 . 0
platform / x86 : dell - wmi : check for kmalloc () errors
cassini : fix off by one
drm / radeon : fix typo in finding PLL params
ASoC : da8xx / omap - l1xx : match codec_name with i2c ids
pinctrl : sh - pfc : add missing of_node_put
mlxsw : spectrum_router : Compare only trees which are in use during tree get
blk - mq : Wake tasks entering queue on dying
mmc : mxcmmc : handle mmc_of_parse () errors during probe
KVM : avoid NULL pointer dereference in kvm_dirty_ring_push
Btrfs : fix an oops when deleting snapshots
mac80211 : Keep skb -> piority for relayed frames in AP mode
[ media ] sony - btf - mpx : Drop needless newline in param description
mmc : Fix mmc_delay () function
f2fs : fix to return correct error number in f2fs_write_begin
powerpc / 64 : Don ' t use early_cpu_has_feature () in cpu_ready_for_interrupts ()
Staging : lustre : module : Replace function calls
[ media ] cec : ignore messages when log_addr_mask == 0
tools : iio : iio_utils : check amount of matches
[ IA64 ] kexec fails on systems with blocks of uncached memory
powerpc / pseries : Optimize IOMMU setup
[ PATCH ] ipw2200 : mark " iwconfig retry 255 " as invalid
block : fix use - after - free in sys_ioprio_get ()
HID : hyperv : match wait_for_completion_timeout return type
jbd2 : fix use after free in kjournald2 ()
ath10k : abort scan properly if wmi_scan_stop fails
ASoC : img - parallel - out : Add pm_runtime_get / put to set_fmt callback
ALSA : hda - Enable widget power saving for Realtek codecs
sctp : Correctly track if AUTH has been bundled .
lock debug : sit tight when we are already in a panic
make sure do_dentry_open () won ' t return positive as an error
ath9k : don ' t run periodic and nf calibation at the same time
bpf : force strict alignment checks for stack pointers
xfs : fix kernel memory exposure problems
crypto : user - fix memory leak in crypto_reportstat
scsi : storvsc : fix SRB_STATUS_ABORTED handling
l2tp : fix info leak via getsockname ()
powerpc : Add missing prototype for hdec_interrupt
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
sched / numa : Fix math underflow in task_tick_numa ()
ALSA : seq : Cancel pending autoload work at unbinding device
[ XFS ] Check return value of xfs_buf_get_noaddr ()
[ PATCH ] cpufreq : fix section mismatch warnings
ALSA : usb - audio : Fix NULL dereference in create_fixed_stream_quirk ()
dm log writes : move IO accounting earlier to fix error path
iwlwifi : Fix memory leak in iwl_req_fw_callback ()
bnx2x : Calling pci_set_drvdata earlier
drm / i915 : Return stored value from max freq sysfs entry
staging : iio : compress return logic
ALSA : hdspm - Fix PCI ID for PCIe RME MADI cards
[ ALSA ] seq_midi_event : fix parsing of F9 / FD bytes
[ SCSI ] ibmvfc : Driver version 1 . 0 . 4
ipv6 : Add GSO support on forwarding path
fs : Rearrange inode structure elements to avoid waste due to padding
Btrfs : add missing kfree in btrfs_destroy_workqueue
seq_buf : Fix overflow in seq_buf_putmem_hex ()
mm : numa : Do not account for a hinting fault if we raced
fm10k : rename mbx_tx_oversized statistic to mbx_tx_dropped
usb : gadget : fsl_qe_udc : Remove unneeded condition
rds : Fix NULL pointer dereference in __rds_rdma_map
staging / lustre / ofd : refill env in ofd_get_info
tproxy : allow non - local binds of IPv6 sockets if IP_TRANSPARENT is enabled
tracepoints : use rcu_ * _sched_notrace
dm kcopyd : avoid pointless job splitting
ath10k : ensure txrx - compl - task is stopped when cleaning htt - tx
drivers / video / igafb . c : introduce lost ' return '
[ MTD ] [ OneNAND ] fix call to onenand_verify when writing subpages
powerpc : Check for valid hugepage size in hugetlb_get_unmapped_area
omap3 : beagle : Fix USB host port power control
[ media ] v4l : vb2 - memops : use vma slab when vma allocation
NFSv4 : Make sure unlock is really an unlock when cancelling a lock
yam : fix a memory leak in yam_siocdevprivate ()
ARM : mxc : platform - mxc - mmc : Fix register region size
powerpc / bpf : Use memset32 () to pre - fill traps in BPF page ( s )
tilegx : Don ' t receive packets when the napi budget == 0
vfs : make gcc generate more obvious code for acl permission checking
[ media ] r820t : Fix IF scale
nvmet : fix space padding in serial number
scanner : don ' t crash on strings containing a NUL byte
fix in thread . call
add sanity check for reading multipart files with no parts (# 840 )
exec : -- cap do not set inheritable capabilities
Fixed possible NULL pointer dereference .
vhost : fix queue number check when setting inflight FD
Enforce a limit on value bytes size
add assert for addition overflow on queue creation (# 225 )
Added stack overflow check for hermes :: vm :: hermesBuiltinApply
ocsp : corrected the comparison of the serial size in OCSP response
Reset the output value on error in _gnutls_x509_dn_to_string ()
eliminated double - free in the parsing of dist points
libnsgif : fix oob in lzw_decode
mm : fix prctl_set_vma_anon_name
fix heap - buffer - overflow in mp4read . c
shaper : fix reallocation
lib / bidi : Fix integer overflow ( found by fuzzing )
encode : fix empty FIELD_2DD_VECTOR
parse : Check for non - empty command / rspfile / rspfile_content
fixed missing return status test error
secvar : fix endian conversion
stream : fix false negative on bad RST
Fixed typo while calculating module path length .
Fixed frame allocation from an awaited frame .
checkpath : fix allocation size of path buffer
cJSON : fix memory leak on object parsing error
[ sipmsgops ] fix codec_delete_XX () parsing ( 2 )
[ sipmsgops ] fix codec_delete_XX () parsing
[ core ] fix parse_via () parsing
[ commissioner ] add network name length check in GeneratePskc () (# 4404 )
Decoder : Fixed error handling for dangling fields
Decoder : Set u1_long_term_reference_flag to 0 for error concealment
Fixed error concealment when no MBs are decoded in the current pic
Decoder : Fix for handling invalid intra mode
Decoder : Initialize default reference buffers for all pictures
Verify that the native handle was created
IGraphicBufferConsumer : fix ATTACH_BUFFER info leak
Fix a wrong check in rfc_parse_data
fixed segfault in GIF reading ; this fixes https :// github . com / pts / sam2p / issues / 67
GenericSource : reset mDrmManagerClient when mDataSource is cleared .
OMXCodec : check IMemory :: pointer () before using allocation
Resolve a merge issue between lmp and lmp - mr1 +
LibCrypto : Don ' t copy the prime test candidates
demux : mp4 : fix buffer overflow in parsing of string boxes .
misc : update : fix buffer overflow in updater
Problem : ZMTP v1 static allocator is needlessly resized
fail HELLO command when SSL is required
resolve BUGFIX handle yet unresolved if - features
Fix a shape inference issue leading to nullptr deref .
Prevent one more div by 0 in TFLite
Prevent an OOB read in ` expand_dims . cc `
Handle one more division by 0 in TFLite .
[ lite ] Add validation check for dilation height / width to be positive integers .
Prevent a division by 0
[ security ] Fix failed shape check in RaggedTensorToVariant .
Prevent dereferencing of null pointers in TFLite ' s ` add . cc `.
Fix the CHECK failure in tf . raw_ops . QuantizeAndDequantizeV2 .
Remove unused out - of - bounds memory access from MTM loader (# 249 )
* Fixed Github Vulnerability Report GHSL - 2022 - 066
[ tls ] Treat invalid block padding as zero length padding
transport / server . cc : Return correct size of decompressed lz4 buffer
bugfix (# 245 ) - long incoming message crashes server
fix possible format string exploit if packet dumping enabled
remove assert that can reasonably be expected to happen
ensure fragment offset is less than total length
store mode : check for truncation
Fix the | name | of PerformanceNavigationTiming
[ scheduler ] Remove implicit fallthrough in switch
gpu : Disallow use of IOSurfaces for half - float format with swiftshader .
android : Stop child process in GetTerminationInfo
Drive out additional flakiness of TouchAction browser test .
Fix a lock re - entry bug in GpuDataManager :: UpdateGpuInfo .
Removing unnecessary DCHECK from SafeBrowsing interstitial .
Add a check to prevent len from going negative in xmlParseAttValueComplex .
return early from ProcessRawBytesWithSeperators () when data length is 0
Hide the " Open PDF in Reader " bubble on navigations .
Disables PanelBrowserTest . MinimizeTwoPanelsWithoutTabbedWindow on
omnibox : Remove unregistered NOTIFICATION_BOOKMARK_MODEL_LOADED from action_box test .
Prevent regular mode session startup pref type turning to default .
Add CHECKs for file descriptors used in select () by InotifyReaderTask
[ DevTools ] Use no - referrer for DevTools links
[ pdf ] Defer page unloading in JS callback .
Handle a bad XSLT expression better .
Tests were marked as Flaky .
Remove an unused variable .
Use the lock when accessing the buffer object .
fix incorrect TexImage3D params w / UNPACK_IMAGE_HEIGHT
Upgrade a TextIterator ASSERT to a RELEASE_ASSERT as a defensive measure .
Fix a crash in HTMLFormElement :: prepareForSubmission .
rm flag and rebaseline
Always call UpdateCompositedScrollOffset , not just for the root layer
ash : Fix right - alignment of power - status text .