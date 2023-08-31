mpeg2dec : Hold ref to buffer while mapped
realmedia : Fix Memory leak in error case
diff : avoid potential null pointer dereference on error
daemon : parted : Don ' t crash if part - get - name called on non - partitioned device ( RHBZ # 1142158 ).
fish : Fix memory leak in inspect /- i code ( detected by valgrind ).
ovs_events plugin : Replace strncpy () with sstrncpy ().
modbus plugin : fix 2 leaks on error paths
write_sensu plugin : malloc + memset -> calloc
bind plugin : plug small leak
perl plugin : Warn if " EnableDebugger " has been used after " LoadPlugin ".
src / utils_avltree . c : Add sanity check to c_avl_destroy ().
Fixed memory leak in collectdclient library .
cpu plugin : Collectd CPU ` steal time ' as reported by new Linux kernels .
teamspeak2 : malloc + memset -> calloc
Added the instance name to varnish API
virt plugin : compare pointer to NULL , not 0
postgresql plugin : Disable queries that do not include an SQL query string .
utils_cache plugin : Fix memory leak .
Print a " starting ..." message on stderr if running in nofork mode , for
fix leak in cc_mslsa . c
don ' t forget to liberate etype
strdup ( 0 ) now returns 0
kadm5_setkey_principal_3 not copying key_data_ver and key_data_kvno
Reposition a trace call which was dereferencing freed memory after
src : remove cpplint error using NOLINT directive
src : fix nullptr dereference for signal during startup
buffer : guard against integer overflow
src : fix use of uninitialized variable
src : make CipherBase :: kind_ const
openssl : fix uninitialized memory access
src : fix minor memleak in preload - modules
crypto : fix memory leak in CipherBase :: Final
cid # 10403 follow - up
fix dumping of javascript in pdf
fixed CID 11651
fix CID # 10540
cid 12207 - fix error state for allocating altstr container
fix previous commit
show errno reason in freshclam on failure ( bb # 923 )
fixing small memory leak .
cid 12123 - fix error state in allocating memory in pdf finalize
High : cib : Fix the feature set check
Dev : crmd : fix typo
Put our CIB into slave mode on exit
Low : execd : avoid memory leak when testing remote key
PE : Remove erroneous if - clause
TE : Detect changes to attributes of the cib object
Low : tools : crm_mon - resolve coverity UNINIT error
PE : Prevent a potential use of NULL
High : cib : Fix huge memory leak affecting heartbeat - based clusters
Low : controller : filter certain synthesized failures from CIB
TE : Always exit via mainloop
High : cib : Correctly free memory when writing out the cib to disk
High : PE : Fix memory leak on processing message ( bnc # 780224 )
Dev : Tools : attrd - Fix value ++ expansion
gui : Show messages as text not html
RPC : strictly require HTTP URI "/"
util : handle NULL pointers passed to git_strarray_free ()
repository : plug memory leak
config : set an error message when asked to delete a non - existent key
remote : don ' t try to create tag annotations as refs / tags / v0 . 1 . 0 ^{}
oid : handle NULL pointers passed to git_oid_shorten_free ()
fileops : fix invalid read
path : remove unused local variable
checkout : free last created directory
refs : fix unused - but - set warning
config : also free the XDG buffer
revwalk : remove tautologic condition for hiding a commit
revwalk : clear first - parent flag on reset
streams : openssl : Handle error in SSL_CTX_new
pool : Take into account malloc overhead & pool page size
repository_head_unborn : clear error when HEAD is unborn
don ' t double free pkt
add extra check
Added an extra check to MLU bounds
utils / transicc / transicc . c : Fix incorrect logical operator
make_absolute_path : return the input path if it points to our buffer
Axe the last ent
refs . c : abort ref search if ref array is empty
blame : don ' t overflow time buffer
block - sha1 : make the size member first in the context struct
make -- find - copies - harder imply - C
fetch - pack : Finish negotation if remote replies " ACK % s ready "
git - pickaxe : fix nth_line ()
fix pack - object buffer size
credential : avoid writing values with newlines
http . c : Remove bad free of static block
git - show : Reject native ref
git - add : warn when adding an ignored file with an explicit request .
refs : prevent accidental NULL dereference in write_pseudoref
git - compat - util : make UNLEAK less error - prone
[ PATCH ] Fix count - delta overcounting
xmkstemp (): avoid showing truncated template more carefully
setup : make sure git dir path is in a permanent buffer
wt - status . c : fix a memleak
http - push : do not SEGV after fetching a bad pack idx file
[ PATCH ] Fix git - init - db creating crap directories .
[ PATCH ] HTTP partial transfer support fix .
Illumos 5311 - traverse_dnode may report success when it should not
imap search : Fixed YOUNGER / OLDER
doveadm user : Avoid potential crashes when running via doveadm - server
imap : Mark DELETE command as potentially closing mailbox .
auth : Fixed non - auth passdb lookup when password had ".< encoding >" suffix .
lib - fs : fs_write_stream_finish ( file , NULL ) is now alias to fs_write_stream_finish_async ( file )
mail_modifylog_mark_synced () didn ' t update mmap
imapc : Don ' t send UID FETCH 1 :* after SELECT if mailbox is empty
lib : hash - format - fix leak on _init () failure path
lib - fs : ostream - metawrap shouldn ' t count metadata header as part of the stream offsets .
squat_trie_lookup (): Make sure we clear result arrays first .
dbox rebuild : If alt directory doesn ' t exist , don ' t fail .
lib - imap - client : Don ' t start another DNS lookup if there ' s already one ongoing .
POP3 : A bit more code for handling nicely unsupported auth mechanisms .
lib - smtp : smtp_server_connection_pending_command_data () - don ' t crash if smtp_parser is NULL
fs layout : Absolute paths as mailbox names shouldn ' t return index with INDEX = MEMORY
doveadm - mail - mailbox : Whitespace fix
lib - index : Fixed sequence lookup of newly created mails in transaction view .
i_stream_stat () should return NULL for closed istreams
fts - solr : Removed deprecated waitFlush parameter from commit .
pgsql : Avoid epoll_ctl () errors when closing connection .
Missing error checking in epoll_create (). Patch by Andrey Panin
snarf : Make sure destination mailbox is open when syncing it .
Die if ( Solaris ) LDAP library returns wrong file descriptor .
auth : oauth2 - introspection lookup error should be internal failure
anvil : Penalty tracking was moving last checksums wrongly with memcpy ().
imapc : Fix crash in mailbox_exists () when LAYOUT isn ' t imapc .
dict - file : Updated dotlock settings .
lib - ssl - iostream : Fixed a memory leak when verifying certificate name .
lib : Add asserts to t_abspath and t_abspath_to
lib - storage : Previous change incorrectly reverted setting parent shared namespace usable .
lib - index : Compiler warning fix
istream - decrypt : Correctly check the header length for v1
lib - storage : mailbox_get_guid () crashed if storage didn ' t support GUIDs .
lib - auth : Don ' t crash in auth_master_ *() lookups if caller hasn ' t created ioloop .
i_stream_read (): Added assert to make sure EOF isn ' t returned too early .
fts : Fixed handling NOT when using lib - fts
fts : Fixed crash with some search queries in empty mailbox .
lib - index : Minor fix to handling atomic inc with INT_MIN value .
DEBUG : buffer_verify_pool () crashed with empty buffer .
push - notification : Fix mailbox unsubscribe event
placeholder -> noselect change broke LIST "" " foo /%" not showing foo /.
quota : Mark the mailbox deleted before starting to reduce quota .
libpassword : Deinitialize hash iterator in password_scheme_detect
lib - http : 32bit system fix .
acl : Crashfix for handling namespaces that don ' t have a paths .
imapc : Avoid duplicate FETCH BODY . PEEK [] when mail is already expunged
Removed accidentally committed debug warning .
lmtp : Fixed lmtp_user_concurrency_limit = 0 to actually mean unlimited
reset modifylog sync mark at the end of opening index file
imap IDLE : Add also 172 . 168 / 12 to private networks .
doveadm - save : Set exit code to EX_TEMPFAIL on open error
lib - storage : mailbox_list_delete_trash () - Don ' t ignore errors
authd : remove some whitespace
dns : make function used only within this unit static
conf : Remove dead store .
libratbox / openssl : Fix possible memory leak with SSL certificate fingerprints
Importing ratbox3 r25213 changeset
openssl : Fixed Curl_ossl_cert_status_request () not returning FALSE
ldap : silence clang warning
file_range : avoid integer overflow when figuring out byte range
imap : Added check for new internal imap response code
fix compiler warning : comparison between signed and unsigned
hostip . c : return immediately from Curl_resolv_timeout () upon expired timeout .
cookie : fix memory leak on oversized rejection
fix compiler warning
typecheck - gcc : allow CURLOPT_STDERR to be NULL too
Fixed another OOM problem , this time with test 64 .
curl_ntlm_msgs . c : revert commit 463082bea4
AddFormData : prevent only directories from being posted
sasl : Fixed a memory leak on OOM
config - amigaos . h : ( embarrassed ) made the line shorter
geez , I ' d ruined Nico ' s socklen_t define , corrected it now
fix compiler warning
conncache : fixed memory leak on OOM ( torture tests )
Curl_pipeline_server_blacklisted : handle a NULL server name
setopt : fix CURLOPT_SSH_AUTH_TYPES option read
get the display host name properly
avoid redundant work when reusing same connection
mbedtls : fix memory leak in error path
winbind : Fixed ntlm_auth expecting eol following commit e17c1b25bc33eb
urlglob : avoid NULL pointer dereference
Bug 592484 - invalid write in HB
Prevent an assertion with invalid Unicode sequences
Fix a g ++ error with -- enable - shell - use - argv
Add a line missed in the merge to 4 . 2 - dev for TRQ - 2058 .
Improved a line of code checked in earlier .
Fixed unit tests .
removed code to free trq_server_ip in trq_main . Not needed
Changed place = core = x to start placing cores
Fix an infinite loop ( patch from Martin Siegert ).
Made it so cores or threads can be allocated on
fixed to memory leaks for error cases in procs_requested .
Revert the previous change - this doesn ' t solve fedora ' s problem .
moved sublock inside master lock
Changed the pbs_tcp_timeout value when reading the protocol
Freed up an allocated pointer prior to a return .
undo partial patch
Fixed problem with torque HA where trqauthd was
pua : safety check on tuple_id len
pua_dialoginfo : do not free dlginfo_cell too early
modules / ims_qos : missing return statement
core : free SE header parsed structure
rtpproxy : use reallocxf () to handle realloc failure
pua : release lock on not finding temp dialog
ctl : free allocated pointer in case of error
tm : define tm_ctx_get () to NULL if no WITH_TM_CTX
dialog ( k ): safety check for lookup in dialog table at shutdown
Fixed two mem - leaks in Path - handling .
mohqueue : fix pointer for RTP statistics error
kazoo : check amqp version
dmq : initialize peer_list to NULL by default
erlang : Fix memory leak
kex : check if uptime value was initialized
tm : Ensure all contact attributes are initialised to NULL when serial forking
modules / usrloc : init _ul_sruid also in mi_child_init
pv : rpc commands updated to use rpl_printf ()
sca : fix snprintf buffer size for Expires header in replies to SUBSCRIBE .
* Fixed test on From URI length . Thanks to Papadopoulos Georgios for
tmx : allocate space to store ending 0 for branch value
modules / lcr : print debug message in case from uri regex matching fails
Added some errorchecking in rs_set_warmth_auto ().
Changed rs_cms_get_transform () to return NULL if CMS is disabled .
Fixed leak in batch_queue_save ().
Fixed camera WB for Nikon D7100 .
Added some sanity checking in gui_drawingarea_motion_callback ().
Made paste defualt to all settings .
Changed a malloc to g_malloc
freeing filetype_str when not needed anymore .
stream_id == MAX_STREAMS is invalid , since counting starts with 0 .
core : Set mpctx -> chapters to NULL at uninit
fastmemcpy path fixed
Initialize frame rate extension fields in mp_header_process_sequence_header
vo_opengl : disable drm_egl autopickup
msg : don ' t prefix slave - mode stuff by default
vo_opengl : don ' t upload potentially uninitialized memory to GL buffer
input : fix off - by - one error
demux_mkv : fix minor memory leak
dvd : add the last chapter
audio : fix small memory leak
vaapi : fix uninitialized value read
check sh_video before seeking ; closes cid 56
wayland : don ' t change set fullscreen twice
eq range mapping - 100 .. 100 => 0 .. 100
vo_opengl : reset surfaces on equalizer updates
vo_opengl : fix extension name
fix scrolling status line in windows
add YAXPAX and YAPAXI exports to msvcr80
workaround ! exit if there ' s no video stream ( hopefully it ' s a proper fix )
ffmjpeg yv12 422 fixed
demux : check whether stream is selected in demux_get_next_pts ()
gl_video : fix refcounting
ao_wasapi : don ' t treat SetDisplayName () failure as fatal
fix memleaks ; patch by andrew calkin from gmail com
Added the connect2Server prototype .
av_log : map libav * debug messages to MSGL_DBG2
vo : mark frame lost after seek reset
fix null pointer check in add_subtitles ,
fix broken term_osd output in update_osd_msg ()
vo_direct3d : fix bug in screenshot code with some pixel formats
af : avoid rebuilding filter chain in another minor case
ehh . frame skipping fixed for - ovc frameno
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
Bugfix : Clear total_bad_nonces when zeroing statistics
fpgautils : Check for fgets error
DevAPI : for_each_ { managed , logical } _proc helper macros
Bugfix : free adhoc string elist element when removing it from list
Remove off by one error .
Bugfix : RPC : Avoid processor - specific details in devdetails
Bugfix : hashbusterusb : Correct return value of hashbusterusb_vrm_unlock
Bugfix : Pass newlines as ASCII ( fixes lack of them in non - unicode builds )
Bugfix : Free stratum nonce1 before replacing it with new value on reconnect
Bugfix : Free shares discarded before beginning submission
Fixed bug in AcpiGetHandle (): searching for a relative path always
Initialized Status , line 386 .
ACPICA : test timeout on global lock interface
acpi : acpica : fix acpi operand cache leak in nseval . c
Hardware : Fix possible recursive locking in hwregs . c
Added alignment and math macros
ACPICA : Fix for Linux source translation
AcpiDump : Allow tables that contain only an ACPI table header .
automated code cleanup
typo in comment
Ran acpisrc source cleanup
Added AML_NAMEPATH_OP to AmlDumpOperand case statement .
Modified various APIs to match our intent .
slirp : Fill TCP / IP header template after SYN reception
scsi : Make device scsi - disk reject / dev / sg *
PPC : e500 : correct params -> ram_size with ram_size
target - cris : Build fix for debug output
usb : queue can have async packets
target - arm : fix unsigned 64 bit right shifts .
xilinx_spips : Fix bus setup conditional check
scsi - disk : fix changeable values for MODE_PAGE_R_W_ERROR
block : Replace u_int8_t , u_int16_t , u_int32_t , u_int64_t by standard int types
posix - aio - compat : Fix async_conmtext for ioctl
host - libusb : Correct test for USB packet state
Initialize a variable in all cases
aio : test node -> deleted before calling io_flush
hw / smc91c111 : Add explicit ' return ' rather than relying on fallthrough
vnc : Fix tight_detect_smooth_image () for lossless case
pseries : Don ' t allow TCE ( iommu ) tables to be registered with duplicate LIOBNs
qcow2 : Fix refcount table size calculation
posix - aio - compat : Plug memory leak on paio_init () error path
pci : memory leak of PCIDevice :: rom_file
petalogix_ml605_mmu : Attach ethernet to machine
hda - audio : fix non - mixer codecs
s390 - ccw . img : Fix sporadic reboot hangs : Initialize next_idx
vl : free err
block / vmdk : do not report file offset for compressed extents
rtl8139 : skip offload on short Ethernet / IP header ( CVE - 2015 - 5165 )
ide : Add forgotten VMSTATE_END_OF_LIST in subsection
virtio : Plug memleak by freeing vdev
XBZRLE cache size should not be larger than guest memory size
mips : missing syscall returns wrong errno
target - tricore : Fix LOOP using wrong register for compare
sheepdog : fix NULL dereference in sd_create
cirrus_vga : remove unneeded reset
tcg / optimize : fix setcond2 optimization
kvm : Drop redundant kvm_enabled from cpu_thread_is_idle
block : Add SG_IO device check in refresh_total_sectors ()
ccid - card - emul : do not crash if backend is not provided
qcow2 : Free allocated L2 cluster on error
qemu / iovec : Don ' t assert if sbytes is zero
vnc : Don ' t assert if opening unix socket fails
tcg - i386 : Allocate call - saved registers first .
block : keep bitmap if incremental backup job is cancelled
vt82c686 : avoid out - of - bounds read
usb : sanity check setup_index + setup_len in post_load
block : Catch ! bs -> drv in bdrv_check ()
virtio : avoid buffer overrun on incoming migration
vhost - user : verify that number of queues is non - zero
target - arm : Fix TCG temporary leaks for scalar VMULL
pulse - audio : fix bug on updating rpos
target - s390x : Add missing tcg_temp_free_i64 () in disas_s390_insn (), opc == 0x8e
i8259 : Do not update IRQ output after spurious pic_poll_read
qcow2 : Free preallocated zero clusters
exec : always use MADV_DONTFORK
xhci : add xhci_cap_write
fw_cfg : add required header files
vfio - pci : Fix MSI / X debug code
block / iscsi : fix uninitialized variable
xen_disk : mark ioreq as mapped before unmapping in error case
target - mips : remove POOL48A from the microMIPS decoding
slirp : Fix packet expiration
handle SD CMD5 without error messages
ehci : schedule async bh on async packet completion
rtl8139 : skip offload on short TCP header ( CVE - 2015 - 5165 )
hw / intc / arm_gic : Fix NVIC assertion failure
qemu : msix nit : clear msix_entries_nr on error
tcg - s390 : Icache flush is a no - op .
vmdk : Fix format specific information ( create type ) for streamOptimized
arm : fix compile on bigendian host
sysbus : make SysBusDeviceClass :: init optional
spapr : Provide an error message when migration fails due to htab_shift mismatch
bb # 11514 - change cli_malloc with cli_calloc
bb # 11129 - applied unencoded '@' patch
dconf : disable swizzor heuristic by default
don ' t cache possible unwanted
fix loop in cab file processing : ensure window position within frame
bz10893 : fixes bug for hash calculations when using -- allmatch .
hwp3 . x : enable password cancelling of parsing
Deal with similar wrap around in literal_len in the src buffer
Related : rhbz # 1602527 add comment to silence leaked_storage for ' glyphs '
partx : work properly with 512 sectors ( dos PT )
sulogin : use EXIT_ *
utmpdump : make analyzers happy
fdisk : add support for the Xtensa architecture
libblkid / minix : Match minix superblock types
blkid : fix typo in ID_FS_AMBIVALEN =
mount : remove useless if - before - free tests
setpwnam : remove non - ANSI definitions [ smatch scan ]
agetty : fix memory leak [ clang - analyzer ]
lib : [ procutils . c ] improve robustness
tailf : fix memmove () usage
sys - utils / ipcutils : be careful when call calloc () for uint64 nmembs
libblkid : fix blkid_partlist_devno_to_partition () for extended partitions
rev : Avoid calling free in a signal handler
sulogin : don ' t run if not found passww entry [ coverity scan ]
login - utils : remove unwanted newlines from wall
libblkid : don ' t use fstat () in blkid_get_dev_size ()
Fix a memleak in torrentmodel .
Fixed typo reported at github # 1552
Settings : add missing save call for the ' shortcut / gkey ' setting .
fix : segfault through python bindings with fd to non - package (# 24089 ).
Fix a small memleak in rpmsign tool
fix parsing of empty doublequoted strings in spec file ( rhbz # 456103 )
don ' t close std [ err , out ] after a verify script
added an error message when rmdir () fails
fixed a memory leak
Log an error on attempt to sign V3 packages ( RhBug : 517818 & others )
bc_eval : legacy scripts may not contain some string list options
conversations : don ' t rename to self !
sync : don ' t count EXPUNGED record annotations in CRC
libconfig : more universal fix for logging fopen () error
Oops , got the sense of a return backwards .
jmap : free memory of ical component clone
http_jmap . c : gracefully handle NULL mailboxes on close
http_caldav . c : dont leak userid in caldav_delete_cal ()
jmap : fix typo in calendarEventsSet response name
log quirky searches
mboxlist : fix uninitialised memory use where pattern is " Other Users "
jmap : getMailboxes : fix sortOrder memory leak
cyr_expire : missing flag is fine
mboxlist : don ' t crash without patterns
strip batchsize on full sync
lmtpd . c : don ' t try local delivery if mlookup () fails
snippets needs a mailbox at the moment too
mailbox : set header size on create so appends work
dav : don ' t double - close davdb on mailbox delete
mboxname : free recipient in cleanup
lmtpd : use xmalloc
buf : don ' t realloc for every buf_ensure
mailbox : cleanup correctly when DELETED records retained
cyr_expire : don ' t resurrect convdb for deleted user
http_caldav_sched : handle blank attendees
cache : if fd has gone away , make sure mmap is clean too
search : always index the target of a rename
mailbox : but if there ' s no partition , then it really is removed
prot : disable boundaries for now
http_caldav . c : don ' t allow an attendee to reply to a non - existent invitation
ctl_zoneinfo . c : skip any files that we can ' t parse as iCalendar
mboxlist : don ' t fail on subscribed but non - existent folder
http_dav . c : reset txn -> buf in MKCOL after we ' re done creating XML response
resolved a problem with delayed loading of keys
Cast sentlen to int before comparison wit bufpos .
cli : Only print elapsed time on OUTPUT_STANDARD
Streams : return an error message if using xreadgroup with '$' ID .
Call clusterUpdateState () after CLUSTER SETSLOT too .
fix recursion typo in zmalloc_usable
adding check error code
Scripting : Reset Lua fake client reply_bytes after command execution .
Fixed possible buffer overflow bug if RDB file is corrupted .
shorten : set invalid channels count to 0
fixing segfault with http :// sam . zoy . org / zzuf / lol - ffplay . avi
avfiltergraph : set deprecated filter_count .
remove unnecessary - 1 from snprintf len
udp : Fix local_port management
indeo4 : fix out - of - bounds function call .
avio : Free URLContext private data allocated via AVOptions
avprobe : free options at the end of main ()
lavf : fix the comparison in an overflow check
workaround apps which dont provided correctly padded streams
avplay : Check frame allocation inside video_thread ()
put_bits : fix invalid shift by 32 in flush_put_bits ()
movdec : Avoid av_malloc ( 0 ) in stss
avplay : free the active audio packet at exit .
mpegtsenc : use correct PES stream_id for AAC
gcc bug workaround by ( Jonathan Gray < khalek at linuxgamers dot net >)
h264 : Use the correct argument context in debug log
mov : Add support for zero - sized stsc runs .
dca : Validate the channel map
oops , potential overflow on really large blocks
sipr : Use memmove () to copy overlapped buffers .
rtmppkt : Make pkt -> data reallocable
xa : fix end - of - file handling
flvdec : Check for overflow before allocating arrays
Check that the seekheads were created
qsvenc_hevc : use the correct HW plugin UID
Pad the extradata in the AAC ADTS to AudioSpecificConfig bitstream filter .
flacdec : set default sample_fmt before parsing extradata
fix size of header data
dfa : check for invalid access in decode_wdlt ().
vsrc_movie : fix leak in request_frame ()
vc1dec : Override invalid macroblock quantizer
rv34 : Avoid NULL dereference on corrupted bitstream
mpegts : Make sure we don ' t return uninitialized packets
png : Support RGBA64 pixel format
prores : Evaluate all the quantizers
libxavs : add an AVClass at the beginning of XavsContext .
dash : Use correct ISO C scanf conversion specifier
anm : prevent infinite loop
adts : Fix PCE copying .
Correct the threshold of get_ur_golomb_jpegls () at which the optimized
xxan : don ' t read before start of buffer in av_memcpy_backptr ().
a64 : check that extradata exists before reading from it
wmapro : return early on unsupported condition
dv : Replace a magic number by sizeof ()
proresdec : Properly make sure an index doesn ' t run past the limit
mpegmux : add stuffing to avoid incomplete PCM frames
avio : Apply avoptions on the URLContext itself as well
avprobe : Zero the allocated avio buffer memory
tinfoil patch : accept no widths that are no multiples of 4
clear the whole vert_pred buffer for 24 bit decoding .
mov : fix stream extradata_size allocation
avcodec : set avpkt -> size to 0 if encode2 () did not output a packet
decode h264 end - of - slice flag
sol : return error if av_get_packet () fails .
reset interlaced_dct flag
lavfi : use designated initializers in avfilter_class
return eof / error if only ffm header has been written , should fix # 815
mpegts : Silence " can ' t seek " warning on unseekable
vaapi_h264 : Add missing return value check
used defined name for testing error resilience level
cbs_h2645 : Return error if writing fails
simplify format string for writing pix_fmt string .
smoothstreamingenc : Check the output UrlContext before accessing it
audemux : Add a sanity check for the number of channels
fix a memleak in av_metadata_set ()
Flush the remaning chars in the input buffer after reading the
cbs_h2645 : Fix memory leak on when reading SEI fails
add a memory check just in case
fix HMAC error report
made paircompare () static
Releases don ' t get replied to
Initialize all of the structure
purple : Fix crash on ft requests from unknown contacts
Catch an infinite loop in smpp_handle_dlist (). Fixes bug 246 .
Call the " without FCS " version of the Ethernet dissector .
Reset the captured - packet count when we start or restart a capture .
GTK : Bugfix not allowing extcap capture filters to be verified .
RTPS : fix Result of operation is garbage or undefined
mqtt : Copy UAT match_criteria value
Removed an unneeded cast .
wlan_stat : fix a memory leak
gcc - 4 . 1 . 1 thinks ' retval ' might not get set inside the TRY (). Initialize it .
dirty hack to make things compile without libgcrypt
frame_data : Reset subnum if visited is resetted
GTPv2 : fix Copy - paste error ( CID 1158587 )
decode as : Fix small memory leak
sprintf big fix
wsutil : return false if NULL is asked as profile name ( CID : 1405202 ).
gtk : Fix null dereference in stats
Add a description for errno EINVAL ( Invalid Argument ) to
DCT2000 : prevent a stack buffer overflow
packet - sip . c : 1469 : warning : unused variable ' contact_param_end_offset '
DICOM : Revert 3752a643ac5c85622822c88c69d83ce23828acb8
ssl_association_remove (): free assoc -> info too
epan_init (): make ' status ' volatile ( to avoid might - be - clobbered warnings ).
gtk : fix memory leak in profile dialog .
dot11decrypt : free memory on exit ( found by clang ).
[ SUA ] Change the dissector table sua . prop . tags to use DISSECTOR_TABLE_ALLOW_DUPLICATE
Qt : Add a bottom border to the packet map .
plugin_if : fix no previous prototype for ‘ ext_toolbar_find_item ’ [- Wmissing - prototypes ]
bgp : add return check ( CID 1354417 ).
Try to make Mac OS X Buildbot Happy
tshark : fix double memory free when analyzing several RTP streams
Stuff the " Don ' t show this message again " checkbox in the dialog ' s
HTTP2 : Fix double free
Qt : add initializers ( CID 1159363 ).
extcap : add more checks for extcap disabled .
From : Susanne Edlund
ws_pipe : Zero an array .
qnet6 : don ' t calculate crc if length is negative
Break out of an infinite loop . Fixes bug 95 .
Qt : Apply recent column widths when columns changed
extcap : fix a compile error with Qt4
get_persconffile_path () return malloc ' d memory ,
RTP : Parse problem in dissect_rtp_hext_rfc5215_onebyte
Add a cast to try and fix the OSX buildbot .
Qt : fix buffer alarm threshold parameter retrieval in multicast statistics dialog
pcapng : Free option_content on error
Add another check to radiotap - iter initialization to catch another out - of - bounds
Fix a memory leak which occured upon registering an already existing
Fixed icorrect key length after updating constant index
fix double free
Fixed an iconv_strrpos () bug that the function doesn ' t return accurate
fix potential leak
Patch by Michael Wallner , signed off by Edin Kadribasic .
fixed php version detection
Added check for calloc () failure
don ' t read empty files
fix typo and bug # 41962
cannot import nodes without document
keep previous test to avoid any behavior change
forgot to decrement result_allocated :(
Initialize all struct members : Necessary for reflection
fix yet another leak
fix memory leak
replace llabs with imaxabs
fileinfo : fix misleading indentation in libmagic
Freeing the memory would be a good idea ...
Fixed heap overflow
IE we love you all ...
cleanup unused variable
oops . Copy - paste - magic . :)
fix for bug # 32282 ( segfault in mysqli_fetch_array on 64 - bit )
Fixed assignment of reference to itself
Added check for small ZEMD_MM_SEG_SIZE
don ' t leak on error
Prevent opening of empty filename ""
Fixed bug codes in SplFileInfo :: getExtension
Fixed a possible memory leak on open_basedir validation
fix segfault when running run - tests . php , and general potential for corruption
efree match_sets on an error here - found by Coverity
oops , 5 . 4 and 5 . 5 use malloc in TSRM
Fixed memory leak
prevent flags overflows
efree match_sets on an error here - found by Coverity
Unterminated paren group in BETHREADS definition of tsrm_tls_set () macro
make ftp compile on unix again
support sucking data from streams in file_put_contents ()
Fixed possible memory leak .
Fixed uninilized intern
dir : Reserve the whole @@ prefix
lib : Make flatpak_installation_drop_caches return TRUE on success
run : Block setns ()
[ common / flatpak - run ] add / dev / mali0 to -- device = dri
update - appstream : Don ' t fail badly if remote has not appstream branch
run : Don ' t allow chroot ()
flatpak_number_prompt : Add missing va_end
run : Don ' t allow unmounting filesystems
create - usb : Fix a leak of the list of extensions
xdp - fuse : Add parentheses to clarify precedence in a conditional
synIDattr (): true color awareness
vim - patch : 8 . 0 . 0584 : memory leak when executing quickfix tests
shada : Fix offset saving
viml / executor : Fix check - single - includes
winpr - handle : fixed mem leak when closing named pipes
core : Added missing \ 0 termination to crypto_print_name
libfreerdp - gdi : fix gdi_Glyph_Free () segfault
core : free polyline and polygon_sc points
Fixing issue 245
Be on the safe side when freeing stuff
Parser : be more conservative when estimating the line size
Reset the ThreadingMsgListModel after setting the previous model , too
getty - generator : fix segfault when $ container_ttys is not set
logind : when enumerating acl devices for seat0 , don ' t set a tag
journalctl : properly detect empty journal files
journalctl : show fields requested with -- field in full
switch - root : do not use close old_root_fd after rm_rf_children ()
shared : time - dst : Avoid buffer overflow
kdbus : make sure msg structure is properly initialized
[ PATCH ] udevd : fix valgrind warning
resolved : return exit code of event loop
[ PATCH ] extend exec_program []
tmpfiles : restore previous behavior for F / f
resolved : merge two bools into a bitfield
modules - load : filter out double modules
systemctl : return diffrent error code if service exist or not (# 3385 )
locale : add printf attribute to log_xkb ()
systemctl : fix memory leak in systemctl_parse_argv
journal - upload : allow the tool to start
sd - event : fix theoretical leak on error in event_make_signal_data
sd - bus : let ' s use mfree () where we can
systemctl : do not print header if no units will be listed
util : look at STDOUT , not STDIN , when determining tty columns
machinectl : don ' t show ". host " pseudo - machine in list by default
systemctl : allow listing cgroups of containers
resolved : use single message for both dbus and signal calls (# 3515 )
login : fix mem leak
libudev : always add UDEV_LOG
[ PATCH ] allow NAME_SIZE > SYSFS_PATH_MAX
shared : handle unnamed sockets in socket_address_equal ()
bus : properly unset default bus pointer when destroying last reference
locale : make sure that l is freed
udev : link - config - fix naming policy
journal : make sure to initially populate the space info cache (# 4807 )
base - filesystem : avoid all searching if the link already exists
build - sys : make gcc shut up
rtnl : fix for 32bits
udev : properly detect reference to unexisting part of PROGRAM ' s result
libudev : fix sysnum logic for digit - only device names
IMPORT : 2048 -> 4096 bytes buffer
udev : kmod - fix typo
logind : never select closing sessions for a VT
resolved : don ' t allow adding of ANY class / type RRs to local zones
ask - password : use kill ( PID , 0 ) before querying a password
json : avoid cleanup of unitialized variable
virt : make sure we don ' t ignore some errors
sd - rtnl : rtnl_call - don ' t dispatch wqueue after timeout has passed
logind : prefer new sessions over older ones on VT switches
path - lookup : use correct cleanup function for strv (# 5007 )
selinux : reopen log when loading the SELinux policy fails
sysctl : parse all keys in a config file
dhcp - server : fix integer underflow
journald : do not free uninitialized pointer in error path
core / transaction : fix cycle break attempts outside transaction
Fix an off by 8
check that ctx is not a null ptr before deref cid # 131
heimdal : Fix the developer O3 build
roken_detach_prep () close pipe
check that ctx is not a null ptr before deref cid # 140
Prototypes for keyfile and unseal_key .
add -- no - embedded - cert and -- embed - leak - only
make compile again
improve msft compiler case
don ' t leak memory
select heimdal_kuser as text domain
make EVP_BytesToKey return an error
kdc : validate sname in TGS - REQ
cryptostats cleanup from Dave Mills .
Fixed loop condition .
settings : GatewayBypassLocal TRUE by default
Fixed NULL dereference .
Fixed RSA generation for OpenSSL >= 1 . 1
rdpei : Fix possible NULL ptr access
Fixed malloc with 0 size .
primitives : renamed NEON detection function
Added assertions to detect invalid call sequences of < transport_free >
Fixed null pointer dereference .
libfreerdp - core : fix server - side transport_attach crash
core : reset crypto key counters when the keys are initialized
Fixed # 6013 : Check new length is > 0
libfreerdp - core : fix server crash in case rdp key file is not provided .
libfreerdp - utils / args : fix wrong argument index for plugin name .
Fixed # 6005 : Bounds checks in update_read_bitmap_data
Fixed oob read in ntlm_read_NegotiateMessage
Fixed GHSL - 2020 - 101 missing NULL check
Fixed missing stream length check in drive_file_query_directory
rdpsnd : follow rdpdr and free the stream from svc_plugin
Fixed return value for DST .
Added assistance file name NULL check .
Fixed # 6011 : Bounds check in rdp_read_font_capability_set
Fixed missing variable type
Deactivated glyph cache .
Fixed Out - of - bound read in glyph_cache_put
libfreerdp - rfx : encoder must increase frameIdx or mstsc would crash .
libfreerdp / core : transport_write unchecked parameters
flooddetach : add nick changes - close # 941
fix php 72494 , invalid color index not handled , can lead to crash
fix authorization_manager_test build
increase timeout on killing
fix BSONObj ownership
assert that you can ' t shard config db
Protect from missing CodeGen .
Init member ( thanks , valgrind ).
Document a parameter .
setForcedLangOptions -> adjust .
std :: vector :: data () is not standartized and on windows it cause build failure .
avformat / asfdec_f : Correct skip to key code
mpeg4 : report frame decoding completion at ff_MPV_frame_end ().
dv : zero dsp before init , this fixes use of uninitialized dct_bits
avformat / movenc : Check input sample count
avcodec / utils : Better check for channels in av_get_audio_frame_duration ()
indeo5 : check against scaleable frames in non scaleable streams .
dxva : DXVA2_ModeHEVC_VLD_Main10 does not support Main
fix segfault with lol - ffplay2 . mpg ( dunno if this is exploitable , probably not easily )
h264_cavlc : check the size of the intra PCM data .
avcodec / options : use av_opt_copy () in avcodec_copy_context () to copy priv_data
vf_hqdn3d : fix uninitialized variable use
cdgraphics : initialize buffer
add support for PCM codec
dshow : avoid integer overflow when using curbufsize
avformat / utils : fix calculating the absolute difference of timestamps
dxva : DXVA2_ModeHEVC_VLD_Main10 does not support Main
avcodec / vorbis : 1 << 31 > int32_t :: max (), so use 1u << 31 instead .
avcodec / dcadec : add an assert to silence a uninitialized variable warning
avcodec / pthread_frame : Fix memleak of AVCodecContext on error
avcodec / atrac3 : Check init_get_bits8 () for failure
avformat / rmenc : Check for av_malloc () failure
avcodec / mpegvideo_enc : Fix possibly exploitable security issue with - bf
jv demuxer : prevent video packet size overflow
check qblur > 1 ( prevent assert failure / segfault )
h264 : reset first_field when current_picture_ptr is reset
avformat / avidec : initialize header array
mvdec : check var_read_string () return value
avcodec / aacdec_fixed : Fix undefined shift
fix crash on non - AltiVec powered machines : MPV_common_init_altivec doesn ' t check mm_flags
rtmppkt : Make pkt -> data reallocable
brstm : fix use of uninitialized variables
smc : Fix overread .
avformat : unref packet after storing it in internal packet queue
arm : fix use of uninitialised value in ff_fft_fixed_init_arm ()
pthread_frame : Unreference hw_frames_ctx on per - thread codec contexts
avformat / nutdec : fix packet end clearing
Make the RTSP muxer use rtsp_hd_out , for consistency
avutil / opt : dont crash on av_opt_set_dict () with NULL
oggparsevorbis : Remove code messing with cur_dts .
tscc2 : fix out of array access
lavu / opt : validate range before dereference
ffprobe : fix use of uninitialized variable
avcodec / lcldec : initialize encoded correctly
lavfi / hqdn3d : avoid use of uninitialized variable .
avcodec / hevc : Silence " warning : ref0 / 1 may be used uninitialized in this function "
avformat / cinedec : Fix DoS due to lack of eof check
mpc8 : fix channel checks
avcodec / hevc : clear filter_slice_edges () on allocation
wmalosslessdec : make MCLMS arrays big enough for what is written into them .
avcodec / binkaudio : clear padding area of packet_buffer
avcodec / g2meet : Also clear tile dimensions on header_fail
lpc : Remove false positive compilation warning about weight being
tests / checkasm / checkasm : Give macro a body to avoid potential unexpected syntax issues
avformat / vfq : fix deadlock in vqf_read_header ()
avcodec / hevc_refs : Check nb_refs in add_candidate_ref ()
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
avformat / mpegenc : Check for av_mallocz () failure
avformat / hls : fix memory leak with non - http segments
ffprobe : fix crash in case - of is specified with an empty string
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
j2kdec : make curtilenum integer .
avcodec / avpacket : Use av_free_packet () in error cleanups
postproc : fix null pointer dereference with invalid option strings
jpeglsenc : Mark codec as init - thread - safe and init - cleanup
indeo4 : Fix global array overread .
avsdemux : check for corrupted data
avformat / utils : avoid overflow in update_stream_timings () with huge durations
avformat / asfdec_o : Check size_bmp more fully
avcodec / wavpack : Fix runtime error : left shift of negative value - 1
h264 : don ' t initialize missing pictures when using VDPAU .
avsdemux : check for out of bound writes
pcx : properly pad the scanline
mpeg4videodec : disable frame multithreading for GMC , its not implemented at all
avcodec / flacdec : Fix signed integer overflow in decode_subframe_fixed ()
gxfenc : fix null ptr dereference
pass strict_std_compliance to audio context too
matroskadec : unset matroska -> done when seeking
h264 : unref leaking error recovery picture
avcodec / g726 : Fix runtime error : left shift of negative value - 2
fraps : check for overread
webmdashenc : Fix potential memory leak
svq1enc : fix signed multiplication overflow
vorbis_parser : Move vp check to avoid a null pointer dereference
bessel : check that the function terminates as expected by av_assert2 ().
psymodel : dont apply lowpass filters with a cutoff close to the nyquist
libavformat / rtpdec_asf : zero initialize the AVIOContext struct
snowenc : check minimum resolution
avformat / http : Use av_freep () avoid leaving stale pointers in memory
avcodec / jpeg2000 : Fixes integer overflow in ff_jpeg2000_ceildivpow2 ()
flacdec : silence several " warning : X may be used uninitialized in this function "
lavfi / frei0r : fix crash when parameters are not set .
mpegvideo : fix null pointer dereference on switching streams
asplit : don ' t leak the input buffer .
avcodec / mdec : Fix runtime error : left shift of negative value - 127
avutil / frame : fix crash with av_frame_unref ( NULL )
matroskadec : only return corrupt packets that actually contain data
bktr : fix warning : missing braces around initializer
hlsenc : Allocate enough space for the pattern string
ffmpeg : fix handling or empty audio packets
ffserver : avoid leaking pathname at exit
avcodec / mpegaudiodec_template : Check for negative e
sgi : check maximum supported resolution
avidec / guess_ni_flag : fix harmless integer overflow
h264 : allow debuging SEI types
s3tc : fix shift overflow by using unsigned constant
avcodec / imc : Fix bitstream buffer padding
samplefmt : avoid integer overflow in av_samples_get_buffer_size ()
avcodec / scpr : Check y in first line loop in decompress_i ()
swresample / resample : Limit filter length
wavpack : check ch_offset
mpeg12 : fixed parsing in some mpeg2 streams
adxdec : Validate channel count to fix a division by zero .
avcodec / hevc_sei : fix use of uninitialized variables in decode_nal_sei_frame_packing_arrangement ()
motionpixels / mp_decode_frame_helper : assert that the first pixel doesnt reuse the last .
avformat / mov : Check extradata before access
avcodec / wavpack : initialize pointers to silence warning about them possibly being uninitialized
libavformat / http : Fix memory leak in get_cookies .
avcodec / tiff : Check for multiple geo key directories
wtvdec : fix integer overflow
avcodec / avpacket : Check metadata key in av_packet_unpack_dictionary ()
avformat / smacker : check for format mismatch more completely
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
electronicarts : check bytes per sample for validity
flvdec : check return value of create_stream ()
lavc / utils : fix extra ASS sanity check in convert_sub_to_old_ass_form ()
twinvq : validate that channels is not <= 0
eatqi : replace break by goto .
avcodec / rv34 : fix crash while seeking on very damaged file
hwcontext_d3d11va : fix crash on frames_init failure
threads : Perform the generic progress cleanup more carefully .
avcodec / mpeg4videodec : Check for bitstream overread in decode_vol_header ()
pcx : round up in bits -> bytes conversion in a buffer size check
sapenc : Free AVStream -> info on cleanup
avcodec / hevc : clear HEVClc when its deallocated in hevc_decode_free ()
Shut up an uninitialized variable GCC warning in VP8 .
avcodec / dvbsubdec : Check for duplicate regions in dvbsub_parse_page_segment ()
wma : Lower the maximum number of channels to 2
kill uninitialised variable warning in mp3_read_probe ()
aes : fix array index out of bounds warning
avcodec / pixlet : Fix reading invalid numbers of bits
vmd : use the PALETTE_COUNT constant uniformly
mjpegdec : Properly set the context colorspace info
avformat / dump : Fix context / level for payload dump
asrc_aevalsrc : Fix use of uninitialized pointer inside av_strtok ()
add check for invalid channel mode when coupling is used
avformat / vqf : check a few more bits in probe
avformat_alloc_context : initialize raw_packet_buffer_remaining_size .
wma : initialize prev_block_len_bits , next_block_len_bits , and block_len_bits .
avcodec / g2meet : Fix order of align and pixel size multiplication .
avcodec / diracdec : Fix integer overflow in INTRA_DC_PRED ()
avformat / omadec : Use 64bit for ret to avoid overflow
avformat / movenc : Write version 2 of audio atom if channels is not known
avcodec / takdec : Fixes : integer overflow in AV_SAMPLE_FMT_U8P output
AMV : Fix possibly exploitable crash .
aviobuf : Handle a NULL buffer in avio_close_dyn_buf
asfdec : prevent possible memory leak in the asf_read_metadata_obj
flvdec : Check for overflow before allocating arrays
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
fix vdpau vc1 interlace modes
rtpdec_qdm2 : Don ' t try to parse data packet if no configuration is received
rawdec : use av_shrink_packet .
rv10 : consider B frames in low delay streams invalid .
lavf / pcm : check size , do not produce invalid packets
avcodec / hevc_refs : fix potential use of uninitialized min_idx
flvdec : allocate large enough buffer so get_bits () doesnt overread
avcodec / truemotion2rt : Check input buffer size
amovie : avoid crash in amovie_request_frame () if no samplesref is available
Remove a NAL unit ' s trailing zero bytes even when dst_length is 1 .
wmaenc : require a large enough output buffer to prevent overwrites
avcodec / put_bits : Assert that size in set_put_bits_buffer_size () does not cause integer overflows
mpegpsenc : show first SCR / DTS at debug level
ape : Fix null ptr dereference with files missing a seekatable .
libstagefright : avoid memory leak
avformat / mpc8 : fix broken pointer math
avcodec / ffv1dec : Check for 0 quant tables
avcodec / h264_refs : Do not set reference to things which dont exist
examples / vaapi_encode : Fix leak on hwframe init failure
lavc / mediacodec_wrapper : fix potential jni global reference leak
avutil / lzo : Fix integer overflow
utils : fix integer overflow with DURATION_MAX_READ_SIZE
avcodec / mlpdec : Fix : runtime error : left shift of negative value - 8
avcodec / adpcmenc : fix integer overflow / undefined behavior in STORE_NODE ()
avformat_open_input (): Add braces to shut up gcc warning .
lzw : check for overread
lavfi : af_amerge options array was not ended with NULL
lavfi / select : make sure avctx is set before closing it .
avcodec / lagarith : disable lag_decode_zero_run_line () and ask for a sample
avformat / mov : Change the type of the r / g / b variables
h264 : Fix memory leak on ff_h264_decode_init () failure
[ PATCH ] Fix crash when initializing multi - threaded decoding for corrupted file .
vc1dec : silence " may be used uninitialized in this function " warning from gcc
Fix an overflow in the predictor .
golomb : fix end of bitstream check
avcodec / utils : Fix signed integer overflow in rc_initial_buffer_occupancy initialization
avcodec / jpeg2000dwt : Check ndeclevels before calling dwt_decode *()
unset Track sets , relying in AVStream -> priv_data and will be freed in av_close_input_file
rtpproto : Check the size before reading buf [ 1 ]
Fix a warning on an uninitialized variable .
lavc / alac : Avoid allocating huge memory blocks for malicious alac input .
aacdec : Fix null pointer crash
proresenc : Write the full value in one put_bits call
libwebpenc_animencoder : zero initialize the WebPAnimEncoderOptions struct
aiff : check block_align in aiff_read_packet
w32threads : with only 1 thread there cant be a active threading mode .
libswscale / tests / swscale : Fix uninitialized variables
mov : immediately return from mov_fix_index without old index entries
msrle : Use FFABS to determine the frame size in msrle_decode_pal4
lavfi : avfilter_merge_formats : handle case where inputs are same
lavc / mjpegenc : check av_frame_alloc () failure .
ensure av_rescale_q () can be calculated ( won ' t divide by zero )
videotoolbox : fix crash when decoding interlaced video with new API
h264 : Fix parameters to ff_er_add_slice () call
smoothstreamingenc : explict cast to avoid overflow
lavf / mpeg : suppress warning : lpcm_header_len may be used uninitialized in this function
avformat / utils : avformat_find_stream_info fix a crash in case of oom
alsdec : only adapt order for positive max_order
avcodec / rawdec : check for side data before checking its size
af_channelmap : fix uninitialized variable use introduced in ba8efac977f4276f05274947b2b67d144cbc965a
avcodec / asvdec : Check buf_size
hwcontext_d3d11va : fix crash on frames_init failure
avformat / mpegts : fix iteration count in add_pid_to_pmt ()
avformat / mxg : Use memmove ()
vaapi_h264 : Add missing return value check
lavc / hevcdec : fix invalid use of ff_get_format ()
avutil / frame : fix av_frame_copy for unknown layouts
avcodec / interplayvideo : Check that enough data is available for opcode_0x9
bmv : check for len being valid in bmv_decode_frame ().
indeo5 : fix division by 0 in ff_ivi_init_tiles ()
lavd / v4l2 : do not fail when VIDIOC_ENUMSTD returns EINVAL without a valid match
fix double free , priv_data is freed in av_open_input_stream
avcodec / webp : Update canvas size in vp8_lossy_decode_frame () as in vp8_lossless_decode_frame ()
avprobe : Fix memory leak
h264 : dont stop parsing NALs without cleanup on DPC .
rsd : limit number of channels
h265_parse : skip zero sized NAL units
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
pmpdec : fix integer overflow
lavf : fix null pointer dereference in rdt
mpegts : fix stack array overread in read_sl_header ()
Fixed segfault on memory allocation failure in ape demuxer .
lavf / concatdec : fix crash in case a file in script cannot be opened
fate / api - codec - param : fix codec context leak
avcodec / mjpegdec : make type of shift unsigned to avoid undefined behavior
avcodec / libxvid : fix potential integer overflow
truemotion1 : use av_freep ()
vf_fps : Fix memory leak introduced by eea64ef4
vp56 : Check for missing reference frame data
avformat / sdp : fix memleak on error
vf_gradfun : make config_props work properly when called multiple times .
avcodec / xpmdec : there are XPM files with dos line endings
adpcm : Fix crash
avformat / cache : Avoid int - overflow in cache compare function
kill uninitialised variable warning in amr_read_packet ()
avcodec / pcm - dvd : reset last header on errors
ffprobe : fix NULL pointer dereference in writer_close ()
cdxl demux : do not create packets with uninitialized data at EOF .
smoothstreamingenc : Fix a memory leak on errors
rtpenc_mpegts : Set chain -> rtp_ctx only after avformat_write_header succeeded
Fix a crash in the H . 263 RTP packetizer
Fix out of bound reads in the QDM2 decoder .
swscale : don ' t use planar output functions to write to NV12 / 21 .
truespeech : check to make sure channels == 1
avcodec / wnv1 : Fix runtime error : left shift of negative value - 1
wmalosslessdec : Reset residues to 0 if they were not decoded from bitstream .
bfi : fix division by 0
tiff : support reading gray + alpha at 8 bits
lavf / utils : Avoid an overflow for huge negative durations .
avformat / oggparseopus : Free opus extradata before reallocating .
avcodec / mpegvideo_enc : Add missing emms_c () to clear MMX state after SIMD use
vc1dec : dont apply the loop filter on fields
mxf : Fix a possible leak of extradata
avcodec / vc1 : reset fcm / field_mode in non advanced header parsing
avcodec / h264 : clear cur_pic structure instead of duplicating it in ff_h264_update_thread_context ()
lavfi : avfilter_merge_formats : handle case where inputs are same
libx265 : Pass through user - set frame type
vc2enc_dwt : use 32 bit coefficients by default
avcodec / x86 / cavsdsp : Put MMX code under mmx check
avformat / mov : Fix DoS in read_tfra ()
h264 : initialize H264Context . avctx in init_thread_copy
hls : Free packets when skipping packets when seeking
vp3 : avoid buffer overread in coeff decode
mpc8 : check seektable size before attempting to use it .
lavfi / aevalsrc : fix sample rate negotiation , after the last merge
indeo5 : fix null pointer crash with ref_mb
avformat / oggparseogm : Check ff_alloc_extradata () for failure
ffserver_config : Check for failure to allocate FFServerIPAddressACL
wavpackenc : assert on too small buffer
checkasm : vp8 . mc : initialize the full src buffer after ec32574209f
vp3 - mt : fix deadlock when first frame is not a keyframe .
avcodec / flacdec : avoid undefined shift
avcodec / pixlet : Fixes : undefined shift in av_mod_uintp2 ()
msmpeg4 : ignore negative DC overflow
lavf : Dont compare absolute to relative timestamps in duration gcd
avutil / imgutils : correctly check for negative SAR components
svq1dec : call avcodec_set_dimensions () after dimensions changed .
h264 : prevent overreads in intra PCM decoding .
dds : disable palette flag for compressed dds
avcodec / tiff : reset geotag_count in free_geotags ()
lavf / cafenc : Only allow mono and stereo for Opus .
avformat / smacker : Check if bufs has been allocated before use
png : use av_mallocz_array () for the zlib zalloc function
rtpdec : Fix the minimum packet length for RTCP SR packets
lavd / lavfi : fix compiler warning for uninitialized variables
RTMP : fix FD leak in rtmp_open ()
Check for out of bound writes in the avs demuxer .
lavf / mov : don ' t read outside frag_index bounds
avcodec / mpegaudiodec_template : Make l3_unscale () work with e = 0
avformat / pcmdec : fix memory leak
random_seed : allow to block on / dev / random
ffmpeg_opt : Use av_realloc_array ()
proresdec : Properly make sure an index doesn ' t run past the limit
avformat / mov : Fix potential integer overflow in mov_read_keys
cast size to int64_t to avoid overflow
Avoid a null pointer dereference on oom when decoding smacker .
avcodec / webp : Fix signedness in prefix_code check
flashsv : check deflateInit () return value
lavc / movtextdec : keep the min size instead of max to fix overread .
mkv : mark corrupted packets and return them
ffserver : free pathname before bailing out
avcodec / dcaenc : clear bitstream end
examples / muxing : check on frame
icodec : fix leaking pkt on error
avcodec / movtextdec : Make tx3g_ptr unsigned
avcodec / hevc_cabac : decrease CABAC_MAX_BIN
avcodec / mpeg4videodec : Clear interlaced_dct for studio profile
avcodec / kgv1dec : Check that there is enough input for maximum RLE compression
avcodec / rl2 : clear freed pointers
Fix a read past end of buffer crash in the mp3 probe
tools / target_dec_fuzzer : Fix return code on open failure
h264 : copy h264qpel dsp context to slice thread copies
avplay : Don ' t try to scale timestamps if the tb isn ' t set
flv : Warn only once
swscale : align vertical filtersize by 2 on x86 .
vorbisdec : check codebook entry count
decode : avoid leaks on failure in ff_get_buffer ()
libavcodec / libtwolame : fix null pointer dereference
bitpacked : fix potential overflow
vda : better frame allocation
pthread : Change a signal to a broadcast as multiple threads
avutil / mem_internal : add missing header includes
avcodec / proresdec2 : SKIP_BITS () does not work with len = 32
avformat / mpegtsenc : Changed Video PES packet length to 0 .
libx264 : do not set pic quality if no frame is output
smoothstreamingenc : fix integer overflow
avcodec / flac_parser : Check for av_malloc () failure
mxfenc : don ' t try to write footer without header
lavfi / abuffer : init the data planes with NULL pointers .
ff_h264_direct_ref_list_init : fix B slice check .
avcodec / dcaadpcm : check for av_malloc () failure
bink : check vst -> index_entries before using it
lavc : use av_mallocz to allocate AVFrames .
rtpdec_asf : Don ' t free the payload context in the . close function
vc1 : reset interlaced variables , prevent another bunch of crashes .
movenc : Free the buffer returned by url_close_dyn_buffer , regardless of the size
vf_unsharp : Fix false ‘ src2 ’ may be used uninitialized in this function warning .
rmenc : more than 2 streams are not currently possible .
mjpegdec : tighten unescaped_buf_size size check , prevent null ptr deref
v4l2 : fix uninitialized variable
lavfi / buffersink : check av_frame_ref () failure .
lzw : fix potential integer overflow .
avformat / mxfdec : Check size to avoid integer overflow in mxf_read_utf16_string ()
libopenjpegenc : add NULL check for img before accessing it
avformat / mxfdec : Fix Sign error in mxf_read_primer_pack ()
avcodec / xsubdec : Check that RLE coded image and colors fit in the buffer
avcodec / ass_split : Fix null pointer dereference in ff_ass_style_get ()
matroskadec : reset size when freeing data .
h264 : free the tables and uninitialize the context on flush
avcodec / mjpegdec : Fix integer overflow in DC dequantization
avformat / mvdec : Use 64bit for ret to avoid overflow
j2kdec : fix division by zero , check tile dimensions for validity
avformat / mxfdec : fix null pointer dereference
jv demuxer : prevent video packet size overflow
bintext : fix possible x variable overflow .
omadec : Properly check lengths before incrementing the position
avcodec / amrwbdec : Fix division by 0 in voice_factor ()
dcadec : check xch_base_channel against channel_order_tab .
h264 : check buffer size before accessing it
cmdutils : fix null pointer dereference
aacdec : drop channel reseting code .
avformat / oggparsedaala : Do not leave an invalid value in gpshift
avcodec / wnv1 : clear padding area of rbuf
oggenc : fix " oggstream may be used uninitialized in this function " warning
libvo - amrwbenc : Add braces to shut up gcc warning .
hevc : fix wpp threading deadlock .
avcodec / mlz : clear dict on allocation to ensure there are no uninitialized values
diracdec : check if reference could not be allocated
avidec : fix " avidec . c : 362 : warning : st may be used uninitialized in this function "
tiffenc : fix bpp value for monow / monob formats
lavc / avfft : init context to 0 .
avcodec / lagarith : fix integer overflow
lavc / videotoolboxenc : Fix crash when closing codec after error
j2kdec : debug markers support .
h264 : Fix a possible overread in decode_nal_units ()
av_tempfile : fix leak in error case
avcodec / eatgv : use av_mallocz () for frame_buffer
lavc / qdrw : Do not fail decoding valid Quickdraw images .
avcodec / dfa : Fix off by 1 error
lavf / movenc : check ff_mov_init_hinting () return
typo me thinks ...
ffprobe : fix uninitialized variable warning
avcodec / smc : fix off by 1 error
avcodec / hevc_cabac : Fix integer overflow in ff_hevc_cu_qp_delta_abs ()
ffm_seek : fix division by zero
h264 : Fix assignments in if ()
a64multienc : change mc_frame_counter to unsigned
jpeg2000 : check len before parsing header
avcodec / lagarith : Check scale_factor
xxan : fix invalid memory access in xan_decode_frame_type0 ()
ffplay : zero initialize copy avpacket
avcodec / libxvid : Fix use of uninitialized AVPacket fields
update_frame_pool : use channel field
adxenc : check return value of avcodec_alloc_frame ()
avformat / sierravmd : check for memory allocation failures
decode : Initialize ret before using it
avformat / msf : Also check the codec tag in probing
mcdec : suppress " warning : a / vst may be used uninitialized in this function "
ra144enc : avoid integer overflows .
wmv1 : check that the input buffer is large enough
avio : check AVIOContext malloc failure
cmdutils : fix uninitialized variable ( type ) warning .
avcodec / wavpack : Fix 280 : 22 : runtime error : left shift of negative value - 1
binkaudio : fix channel count check
avformat / img2dec : zero probe buffer
lavf / concatdec : do not transfer custom IO flag
xan : Prevent NULL dereference with missing palette
avcodec / opusdec : check alignment , misalignment could lead to crashes with avx
dvbsubdec : fix division by zero in compute_default_clut
ffprobe : replace av_destruct_packet () with av_free_packet ()
avcodec / nvenc : set correct error code
avcodec / ivi_common : use av_mallocz () to allocate mbs array
pictordec : pass correct context to avpriv_request_sample
msmpeg4dec : check w / h , prevent assert failure later
avcodec / g2meet : check framebuf has been allocated before use
fix memory leak in oggenc
flvdec : fix creation of lots of phantom data streams
h261 : check mtype .
lavc / jpeglsenc : fix allocation in case of negative linesize , and add malloc check
mp3dec : Fix possibly exploitable crash
matroskadec : add assert on lack of overflow in pkt_size + offset
graphparser : add a NULL check on the argument passed to strstr
Fix out of bound writes in fix_bitshift () of the shorten decoder .
rtspdec : Fix use of uninitialized byte
check entries against field_size , potential malloc overflow in read_stsz , fix # 1357
libx264 : zero x264_picture before use .
avcodec / ra144dec : Fix runtime error : left shift of negative value - 17
avidec : Check that the header chunks fit in the available filesize .
avformat / gxf : Use 64bit for res to avoid overflow
avcodec / alsdec : Clear MPEG4AudioConfig so that no use of uninitialized memory is possible
wmapro : error out on impossible scale factor offsets
hevc : use av_mallocz () for allocating tab_ipm
avresample : Make sure the even check does not overflow
avutil / mem : Fix flipped condition
ffplay : do not drain existing filters when seeking
fixup_vorbis_headers : add missing malloc failure check
avcodec / dcaadpcm : fix use of uninitialized variable
avcodec / hnm4video : check that packetsize is at least 8
bitstream : fix a crash in case of oom
mpeg12dec : reset data size after parsing extradata .
avcodec / dxv : Check remaining bytes in dxv_decompress_raw ()
avcodec / gdv : Fix undefined shift
lavfi / amerge : drop duplicated " Buffer queue overflow " message
vf_gradfun : fix uninitialized variable use
postproc / postprocess : Check for av_mallocz () failure
mpegvideo : Avoid 32 - bit wrapping of linesize multiplications
cook : silence some signed overflow warnings .
idcin : check chunk_size value before using it
avcodec / wavpack : Fix integer overflow
lavfi / mp : fix invalid read with filters with no argument .
avcodec / jpeg2000dwt : Fix runtime error : left shift of negative value - 123
avcodec / crystalhd : allocate padding for extradata
fix memleak in packet corrupt dropping code
avcodec / mlz : Remove ' l ' postfixes from numbers
brstm : reject negative sample rate
tools / target_dec_fuzzer : Remove FuzzerInterface . h dependancy
xavs : Do not try to set the bitrate tolerance without a bitrate
vc1dec : fix block_off
binkaudio : fix channel count check
lavu : prevent overflow in av_clip_intp2_c
avformat / apngdec : Use 64bit for ret to avoid overflow
avcodec / dcadsp : Fix runtime error : signed integer overflow
avcodec / wavpack : Fix runtime error : left shift of negative value - 5
avformat / thp : Check av_get_packet () for failure not only for partial output
v410enc : fix output buffer size check
vp3 - mt : fix deadlock when first frame is not a keyframe .
dpcm : Fix invalid writes
lavf / http : Remove superfluous parenthesis .
lavd / xv : fix memory leak
id3v2 : fix compiler warning for uninitialized variables
ac3dec : ensure get_buffer () gets a buffer for the correct number of channels
avformat / movenc : Check packet in mov_write_single_packet () too
avformat / http : fix memory leak in parse_cookie .
avcodec / truemotion2 : Fix passing null pointer to memset ()
jpeg2000 : Check that nreslevels2decode has been initialized before use
lavfi / volume : fix NULL reference in filter_frame ()
avcodec / tiff : Fix use of uninitialized off variable
rle_code can overflow when multiplied by 4
v4l2_m2m : Fix integer overflow in timestamp handling
avutil / timecode : Fix fps check
mpegvideo_enc : remove assert that has become obsolete with the new API
h264 : prevent rbsp_buffer values from becoming duplicated
avformat / svag : fix division by zero
avutil / mathematics : make av_gcd more robust
avcodec / mjpegdec : Fix runtime error : left shift of negative value - 511
lavc : fix recoded subtitles end .
ffv1 : make sure gob_count is not 0
pmpdec : make i unsigned , avoid undefined behavior of i ++
aacdec : Fix an off - by - one overwrite when switching to LTP profile from MAIN .
avformat / oggparseogm : check input size before reading t
avformat / nutdec : Check ff_gen_search () for failure
avformat / apngdec : Use 64bit ret to avoid overflow
pthread_frame : don ' t sync items between threads for intra - only codecs .
avcodec / utils : fix memleak on avcodec_open2 () failure
avcodec / wavpack : Change wp_log2 () to unsigned
pngdec : do not release buffer on failure instead report full progress
swresample : fix negative rematrix volumns
lavc / avpacket : Fix undefined behaviour , do not pass a null pointer to memcpy ().
tiny_psnr : fix range calculation for sample size of 32 bits
utils : Do not expand a macro with ' defined ' in it
avcodec / wma : Print more details in case of spectral RLE overflows
tta : fix off be 1 error in the end detection .
lavf / id3v2 : do not export empty fields .
libavformat / flvdec . c : free always the packet after a resync .
dvbsubdec : handle mode change page state
avutil / internal : Do not enable CHECKED with DEBUG
get_bits : make cache unsigned to eliminate undefined signed overflow .
avcodec / snowenc : Clear MMX state after edge drawing and picture encode
dashdec : Only free url string if being reused
avcodec / ass : check for av_mallocz () failure
electronicarts : check timebase , fix assertion failure
ffmpeg : Replace goto redo on decode fail with continue .
lavf / segment : exit immediately in case of invalid stream specifier
fix mem leak
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
avcodec / cbs_jpeg : Check length for SOS
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
avformat / img2dec : do not rewind custom io buffers
avformat / riffenc : Use size_t for strlen in ff_riff_write_info_tag ()
apetag : fix error handling in ff_ape_parse_tag ()
Fixed a segfault in the DCA decoder with corrupted streams .
avformat / oggparsetheora : zero extradata padding area
avcodec / shorten : Check residual size
mpegpsenc : Fix integer overflow of the muxrate calculation .
fate / cabac : replace uninitialized bytes by random bytes
mpegaudidec : 10l forgot buf_size
avformat / mxfenc : Set color siting to 0 for D10 - MXF
avcodec / mpegaudiodec_template : Fix multiple runtime error : signed integer overflow
avcodec / libtheoraenc : Check for av_malloc failure
avcodec / a64multienc : use av_frame_ref instead of copying the frame
avformat / mov : Fix signed integer overflows with total_size
shorten : pad the internal bitstream buffer
sws : fix assert failure
cook : Make constants passed to AV_BE2NE32C () unsigned to avoid signed overflow .
ffprobe : fix leak in show_packets ()
flvenc : silence bogus warning
ffprobe : set writer context to 0 after allocation
avcodec / ffv1dec : Explicitly check read_quant_table () return value
h264 : check that execute_decode_slices () is not called too many times
sonicdec : fix frame size
avcodec / ac3enc : fix undefined negative left shift
snow : zero scratchbuf .
avcodec / tiff : Clear deinvert_buf_size on deallocation
avutil / tree : fix memleaks
hlsenc : make segment number unsigned
hevc_ps : make sure failing to decode an SPS always returns an error
aacdec : fix " may be used uninitialized " warning
avformat / dss : Use av_freep () to avoid leaving stale pointers in memory
jvdec : check videosize
avformat : Fix Pro - MPEG non - square matrix
avformat / test / fifo_muxer : add check for FailingMuxerPacketData alloc
lavf / dnxhd : Autodetect more valid files .
avcodec / lossless_videodsp : add_hfyu_left_prediction_int16_c : fix harmless integer overflow
avformat / avidec : Check nb_streams in read_gab2_sub ()
avformat / jvdec : clear packet padding after downsize
avcodec / tdsc : use ff_codec_open2_recursive ()
proresdec_lgpl : explicitly check coff [ 3 ] against slice_data_size
vp9 : reset segmentation information on context reset .
mxfdec : Fix a potential DoS vector in mxf_read_pixel_layout ()
vorbisdec : fix heap buffer overflow .
avformat / mp3dec : fix msan warning when verifying mpa header
avcodec / aac : fix () in IS_CODEBOOK_UNSIGNED macro
avcodec / h264_slice : don ' t sync default_ref [] between threads .
avformat / vqf : check avio_read () s return value more completely
applehttp : Avoid reading uninitialized memory
avcodec / pixlet : fixes integer overflow in read_highpass ()
avformat / hevc : fix mix of av_malloc () with free ()
movdec : Free the previous extradata
avformat / ape : Use av_malloc_array ()
h264 : fix variable overflow after a few years of video playback
lavu : test for broken binutils on ARM
dcadec : Always initialize return variable
swr / rematrix : fix C99 left shift overflow
doc / examples / decoder_targeted : Limit max pixels for fuzzing
hls : do not access pb -> opaque for custom IO .
h264 : on reference overflow , reset the reference count to 0 , not 1 .
matroskadec : fix a sanity check .
latmdec : Check AudioSpecificConfig length before decoding extradata .
af_amix : prevent memory leak on error path
avutil / opt : fix av_log type
vp56 : check buffer size to fix a potential segfault
lavf / mov : fix crash in mov_read_sidx
wmadec : Verify bitstream size makes sense before calling init_get_bits .
avcodec / wavpackenc : print channel count in av_log call
avcodec / pixlet : check out of bounds pfx value
h264 : correct ref count check and limit , fix out of array accesses .
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
checkasm : v210 : Fix array overwrite
avformat / movenc : Do not pass AVCodecParameters in avpriv_request_sample
avresample : prevent theoretical division by zero
avcodec / eamad : silence uninitialized variable warnings
wmadec : avoid infinit loop .
indeo4 : update AVCodecContext width / height on size change
avcodec / hnm4video : change width / height to int
avformat / matroskadec : use av_realloc_array ()
buffersrc : fix invalid read in uninit if the fifo hasn ' t been allocated
Prevent scalefactors from overflowing .
avcodec / webp : Fix null pointer dereference
lavfi : fix use - after - free in ff_filter_frame .
mov : detect EOF in mov_read_dref ()
avformat / pjsdec : dont increase pointer when its already at the end in read_ts ()
avs : silence " may be used uninitialized " warnings
matroskadec : prevent access of elements after freeing
pthreads : reset got_frames on flush .
fixing mem corruption
fix mem leak
avformat / dump : Fix integer overflow in av_dump_format ()
avformat / mxfdec : Fix memleak of packages_refs
golomb : always check for invalid UE golomb codes in get_ue_golomb
avcodec / smvjpegdec : return the packet size instead of 0
avcodec / dvdsubdec : error on bitmaps with size 0
avformat / idcin : Use 64bit for ret to avoid overflow
hwcontext_dxva2 : initialize D3DDISPLAYMODEEX correctly
aacenc : fix out of array writes
avcodec / hevc_ps : prevent stale pointer in malloc failure case
avcodec / ffv1dec : Fix out of array read in slice counting
avcodec / cllc : Check prefix
examples / demuxing : free AVPacket after usage
g723_1dec : remove unneeded cliping that leaked in from merge from libav
lavf / audiointerleave : check for allocation failure
avcodec / msrle : check return code for success before use
avformat / mxfdec : cleanup if parsing the header metadata fails
libavformat / mov : Fix memory leak , need to free the chapter tracks array
wtvdec : fix integer overflow resulting in errors with large files
avformat / 4xm : shrink packet if it was only partially initialized
avformat / utils : check for malloc failure
lavf : avformat_seek_file (): validate stream_index .
Prevent calling init_vlc () with invalid parameters in motionpixels decoder .
avcodec / vp8 : Fix null pointer dereference in ff_vp8_decode_free ()
avcodec / fmvc : Fix off by 1 error
ffmpeg : Clear error message array at init .
lavc / ffjni : fix local reference leak
imgconvert - test : add avg bits per pixel
avutil / frame : Copy size = 0 side data in ff_init_buffer_info ()
aacenc_ltp : fix assertion
pmpdec : Check for zero audio packets .
mpegvideo_enc : remove assert that has become obsolete with the new API
Fix a mem leak in vc1_decode_frame ().
avcodec_encode_audio (): fix invalid free
ffmpeg : disable autoinsertion for - async on the output side of the filtergraph .
avcodec / jpeg2000dec : zero Jpeg2000QuantStyle structure before use in get_qcd ()
uninitialized var ...
dump_stream_format : fix division by 0
lavfi / fade : Do not overread input buffer .
mxf_set_audio_pts : fix division by 0
hdsenc : Fix an off by one error in an array size check
lavfi / showwaves : do not set on outlink -> out_buf the sent video frame
lavf : Make sure avg_frame_rate can be calculated without integer overflow
fix exploitable buffer overflow
use filter name when graph parser add filters
lavfi / alphamerge : fix leak introduced in 217163eb
avf_showwaves : fix null pointer dereference
shorten : Fix out of bound writes in fix_bitshift ()
postproc / postprocess : Check for av_malloc () failure
avcodec / webvttdec : Fix uninitialized use of variable " again "
avcodec / zmbv : Check that the buffer is large enough for mvec
matroskadec : fix a memory leak
lavf / ftp : fix possible crash
ff_gen_search : make step 64it to prevent hypothetical integer overflow
tm2 : Check remaining size before init_get_bits ()
ffmpeg : Check for subtitle_out malloc failure
avcodec / wmalosslessdec : deallocate uninitialized frame on decode_tilehdr () failure
xwddec : prevent overflow of lsize * avctx -> height
avcodec / sheervideo : Check input buffer size before allocating and decoding
atrac3 : Fix crash in tonal component decoding .
id3v2 : allocate large enough buffer
avcodec / snowdec : Check qbias
timecode : fix typo
lavc : do not leak frames in avcodec_decode_audio4
avcodec / mpeg4videodec : Clear mcsel before decoding an image
lavfi / lut3d : avoid overread .
avformat / rtsp : Fix potential pointer overflow in sdp_probe ()
avcodec / mpeg4videoenc : Use 64 bit for times in mpeg4_encode_gop_header ()
af_aresample : fix leak on alloc failure
libavcodec / wmalosslessdec : add missing emms
lavc / libopenjpegenc : check av_frame_alloc () failure .
decode : Initialize ret before using it
avcodec / wmalosslessdec : shrink output on error so no uninitialized data is returned
cbs_h265 : Fix reading of unknown parameter set extension data
vc1dec : prevent memory leak in error path
svq3 : Avoid a division by zero
ffmpeg : fix a crash with complex filters when pix_fmt is not specified
oggenc : add missing initializer braces to shut up gcc warning .
h264 : Mark previous field as done before throwing it away .
avcodec / h264_mp4toannexb_bsf : Reorder operations in nal_size check
lavc : add 422 / 444 YUV with alpha to align_dimensions ()
mpc7 : check for allocation failure
matroskadec : avoid potential mem leak
avcodec / libstagefright : Check for pthread_create () failure
lavf : don ' t leak corrupted packets
ff_msmpeg4_decode_init () calls ff_h263_decode_init () which calls
jpeg2000 : zero reslevel array on allocation
ac3dsp : fix loop condition in ac3_update_bap_counts_c ()
avformat / msf : Check channels when reading the header
rtpdec_h264 : Free old extradata before clearing the pointer
ffserver : Clear avio context after closing it
avcodec / mjpegdec : Check escape sequence validity
mss1 : improve check if decoded pivot is invalid
avcodec / dca_lbr : Fix " warning : missing braces around initializer "
aacdec / lcg_random : fix harmless integer overflow
avcodec / hevc : make * ps_id unsigned
mpegenc : simplify muxrate calculation
avcodec / h264_slice : Fix signed integer overflow
check udp_read_packet return value not to access uninitialized memory
avcodec / mlpdsp : Fix signed integer overflow , 2nd try
avidec : use actually read size instead of requested size
avcodec / hevc_cabac : Check prefix so as to avoid invalid shifts in coeff_abs_level_remaining_decode ()
yuv2rgb : fix integer overflow in fill_table
av_image_copy : check for av_image_get_linesize () failure .
avcodec / dirac_vlc : Fix invalid shift in ff_dirac_golomb_read_32bit ()
vf_pad : check that we have write permission before writing in the buffer .
avcodec / hevc_ps : Check return code from pps_range_extensions ()
put_bits : fix invalid shift by 32 in flush_put_bits ()
avpacket : fix size check in packet_alloc
wmalosslessdec : Fix reading too many bits in decode_channel_residues ()
avformat / mux : Apply auto bsfs in av_write_frame () too
xwma : check bytes_per_sample , fix division by 0 .
avcodec / dvdsubdec : Fix off - by - one error
avcodec / flacdec : Fix undefined shift in decode_subframe ()
avcodec / exr : fix null pointer dereference
asfenc : fix leaking asf -> index_ptr on error
dsicin : Add some basic sanity checks for fields read from the file
vp9_raw_reorder_bsf : Remove a redundant allocation
avformat / vqf : Use 64bit for ret to avoid overflow
avcodec / libfdk - aacdec : Correct buffer_size parameter
pngdec : Fix interlaced PAETH prediction
oggparseskeleton : Check the overall start time before using it .
avcodec / mpeg_er : Clear mcsel in mpeg_er_decode_mb ()
rawdec : propagate pict_type information to the output frame
segafilm : fix division by 0
shorten : dont leave invalid channel counts in the context .
Check for out of bound reads in the Electronic Arts CMV decoder .
targa : prevent integer overflow in bufsize check .
apetag : propagate errors .
vp8 : avoid race condition on segment map .
dca : Fix uninitialized variable warnings .
matroskadec : don ' t warn about unknown spherical medata when none is present
avformat / segafilm : check 2nd tag
cafdec : fix overflow checking in read_header ()
avcodec / utils : Check close before calling it
sgidec : safer check for buffer overflow
asf : reset side data elements on packet copy .
pcmdec : check that channels is valid .
sol : return error if av_get_packet () fails .
iff : validate CMAP palette size
examples / avio_reading : fix null dereference on error
rdt : Check the return value of avformat_open
lavfi / fifo : add assert to ensure request was successfull .
avcodec / ulti : Check number of blocks at init
avcodec / pngdec : Clean up on av_frame_ref () failure
ape : Use the proper variable type
avformat / thp : check av_get_packet () for failure
ffmpeg : avoid NULL dereference with filters .
swscale : fix 16 - bit horizontal scaling underflow .
lavf / img2dec : Initialize a stack variable .
avformat / lrcdec : Fix memory leak in lrc_read_header ()
audemux : Add a sanity check for the number of channels
libopusenc : check return value
dv : Initialize encoder tables during encoder init .
avconv : free packet in write_frame () when discarding due to frame number limit
avcodec / truemotion2rt : also export key frame info
mpc8 : check stream count before accessing stream 1 .
dvbsubdec : add missing break
avcodec / cinepakenc : Zero AVPictures , silence " may be used uninitialized " warnings
avformat / mpegts : reset last_ver on corrupted packets
avformat / sapenc : Use av_mallocz_array ()
mxfdec : Fix files > 2 GiB
avcodec / wavpack : Treat the first block coding too many channels as an error
g723_1 : fix off - by - one error in normalize_bits ()
audemux : Fix potential integer overflow leading to a division by 0
wavpack : Fix an integer overflow
vaapi_encode : Discard output buffer if picture submission fails
ffprobe : check av_asprintf () for failure
mpegts : fix demuxing PES private stream 2
libshine : fix support for shine 3 . 0
svq1dec : Fix overread on very small input
avcodec / ff_h264_decode_sei : fix integer overflow with size .
mov : Fix negative size calculation in mov_read_default ().
avformat / bintext : protect against potential overflow of chars_per_frame
avcodec / snow : Initialize spatial_decomposition_count to a valid value
ipmovie : do not read audio packets before the codec is known
avformat / icodec : Fix crash probing fuzzed file
lavr : check that current_buffer is not NULL before using it
cpu detection : avoid a signed overflow
sanm : remove " duplicate " for loop .
lavfi / separatefields : fix frame leak
libavformat / rtpenc_mpegts : check avformat_new_stream () return value
ac3dec : apply dynamic range compression to correct channels in dual - mono mode .
libx265 : Fix use of uninitialized input picture
avcodec / indeo4 : Check remaining data in Pic hdr extension parsing code
ac3_parser : add required padding for GetBitContext buffer
aac_parser : add required padding for GetBitContext buffer
avcodec / ffv1dec : Fix integer overflow in read_quant_table ()
vc2enc : increase the starting value of the size scaler
avcodec / g2meet : check available space before copying palette
avformat / mov : fix integer overflow in mov_read_udta_string ()
vaapi : return early from ff_vaapi_render_picture () without picture
atrac3 : Fix crash in tonal component decoding .
avcodec / jpeg2000 : Check sum of sizes of band -> prec before allocating
lagarith : pad RGB buffer by 1 byte .
Fix a mem leak in av_find_stream_info ().
bethsoftvid : check return value of av_packet_new_side_data ()
avformat / movenc : Fix use of uninitialized variable ( ret )
apetag : do not leak memory if avio_read () fails
matroskadec : fix leak on error
avcodec / utils : Fix potential overflow in overallocation code
avformat / movenc : avoid using uninitialized display_matrix_size
avformat / oggparseopus : Fix Undefined behavior in oggparseopus . c and libavformat / utils . c
quickdraw : Skip the empty 512 byte header for images
utils . c : fix crash with threading enabled .
vp56 : alpha_offset is uninitialized on purpose
avformat / oggdec : Fix integer overflow with invalid pts
lavf / utils : fix mem leak
fix initial vbv buffer underflow
avformat / libquvi : Fix whitelist handling
af_join : do not leak input frames .
avprobe : Zero the allocated avio buffer memory
mpegpsmux : Fix integer overflow with huge VBV sizes .
jvdec : check that the video_size fits in the packet .
avcodec / cllc : Check bitstream end before decoding pixels
avformat / rtpenc : check av_packet_get_side_data () return , fix null ptr dereference
avformat / mov : fix avg_frame_rate calculation
rtsp : Fix compiler warning for uninitialized variable .
mpegenc : prevent a NULL pointer dereference
motionpixels : Prevent calling init_vlc () with invalid parameters
ffmpeg : Fix bsf corrupting merged side data
crypto : Use av_freep instead of av_free
h264 : don ' t initialize missing pictures when using a hwaccel
mjpegenc : Remove duplicate initializer
udp : do not call pthread_ { mutex , cond } _destroy when not initialized .
avcodec / h264 : Clear delayed_pic on deallocation
avcodec / bmp : fix integer overflow in checking header sizes
vp9 : make mv bounds 32bit .
avcodec / wmalosslessdec : fix mclms_coeffs * array size
vf_hqdn3d : make config_props work properly when called multiple times .
lavc / options : fix shallow copy context
rtmpproto : Validate the embedded flv packet size before copying
avcodec / takdec : Fix integer overflow in decode_lpc ()
avcodec / diracdec : Check perspective_exp and zrs_exp .
ffprobe : fix use of uninitialized pointer in av_strtok ()
diracdec : Test mctmp and mcscratch for malloc failure .
avcodec / mpegvideo_enc : check that max_b_frames is not negative
avcodec / vp3 : check current_frame before accessing it
mpegts : Fix potential memory leak in mpegts_read_packet ()
fixed long standing off - by - one bug ( fixes playback on ppc )
libx265 : Fix use of uninitialized input picture
avcodec / jpeg2000dec : Check SIZ dimensions to be within the supported range
wmaprodec : check num_sfb for validity . Fix out of array accesses
avformat / vqf : check number of channels before use .
sdp : Check that fmt -> oformat is non - null before accessing it
matroskadec : fix out of bounds write
lavf / utils : Do not force chapter end time before chapter start .
avformat / flac_picture : allocate buffer padding for picture
avcodec / aacdec_fixed : Fix integer overflow in apply_dependent_coupling_fixed ()
lavf / utils : ignore subtitle timestamps in timings calculation
xan : Prevent NULL dereferences with missing reference frame
pthread_frame : Unreference hw_frames_ctx on per - thread codec contexts
drawtext : draw glyphs for the strftime ()- expanded text
examples / filtering_audio : fix a memory leak .
check for sane values in ogg_get_length (), fix lol - ffplay . ogm fuzz test
mp3enc : fix CODEC_ID / CodecID that leaked in from older patch
avformat / ipmovie : Check that the OPCODE_INIT_AUDIO_BUFFERS size is large enough
matroskadec : use uint64_t instead of int for index_scale
riff : fix division by zero for G726
ffmpeg : Check read_ffserver_streams () return value
Free the RTSPStreams in ff_rtsp_close_streams
avcodec / interplayvideo : Fix timeout from lack of bitstream end check
avcodec / dvdsub_parser : never return 0 when the input isnt 0
yop : check return value of avformat_new_stream ()
dds : disable palette flag for compressed images
lavc / audiotoolboxdec : fix memory leak
H264 : hotfix for speedloss on frame threading and h264 files with slices .
avformat / mov : zero initialize codec_name in mov_parse_stsd_video ()
avformat / ipmovie : Check OPCODE_CREATE_TIMER size
Fix a crash on oom when decoding hevc .
avcodec / x86 / dsputil_init : only use xvid idct for lowres = 0
avformat / mov : clear padding area in mov_read_extradata ()
avformat / flvdec : initialize context before reading from it
rv30 : check block type validity
jvdec : Return EOF on end of file
avcodec / mmvideo : Fix undefined behavior ( left shift of negative value )
avcodec / libxavs : Use av_mallocz_array ()
avcodec / mpeg12dec : Fix runtime error : left shift of negative value - 1
Remove lowres support from wmv2 decoder
rv34 : Avoid NULL dereference on corrupted bitstream
indeo4 : check ref_mb
rtpdec : Limit writing to the buffer size
avcodec / tiff : remove byte based bpp special case
r3d : check that sampling rate is non negative .
avcodec / pcm : Fix overflow in bitrate computation
opt : fix memleak on error of range_array
avutil / opt : ensure the right buffer is used in set_string_number ()
flvdec : skip duplicate indexes
avcodec / mlpdec : Check that there is enough data for headers
libxvid_rc : fix leaks in ff_xvid_rate_control_init ()
avutil / hwcontext_d3d11va : Fix leak when wrapping texture in AVD3D11FrameDescriptor
avcodec / imgconvert : fix possible null pointer dereference
hdsenc : Avoid integer overflow
ffplay : add existence check on ic -> pb in decode_thread ()
lpc : use default number of passes when lpc_passes is invalid
rtspdec : read_line : fix use of uninitialized byte
dpx : Make start offset unsigned
allcodecs : move libaacplus down .
avcodec / nvenc : fix potantially uninitialized free
avcodec / vp9_superframe_bsf : cache packets by creating new references rather than moving them
g726 : Do not try to decode more than one channel .
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
mov_probe : fix integer overflows
avformat / dashdec : Fix memleak when resolve_content_path
avformat / iff : check avio_read () return in get_metadata ()
avidec : Free packet if dv_produce_packet fails .
dsicinav : Check for out of bounds writes
xwddec : prevent overflow of lsize * avctx -> height
avcodec / vda_h264 : fix null pointer dereference
perform sanity check on number of channels in electronicarts demuxer
avcodec / avpacket : zero memory in av_packet_new_side_data ()
Avoid a null pointer dereference on clean - up after oom in ac3 encoder .
cbs_h2645 : Fix memory leak on when reading SEI fails
avcodec / hevc_ps : Fix c ? _qp_offset_list size
avcodec / gdv : Check available space before reading palette
avoid possibly uninitialized return value
mem : Make av_strdup allocate using av_realloc
avformat / adxdec : dont return random data on EOF
audio_mix : initialize the data pointers to NULL
yuv2rgb : fix integer overflow in fill_gv_table ()
lavf / matroskaenc : fix uninitialized read
avformat / smoothstreamingenc : Use av_mallocz_array ()
diracdec : clear slice_params_num_buf on allocation failure
smush : check audio packet size
avcodec / mpegvideo_enc : Fix leak on bitstream buffer reallocation
VP3 : Prevent stack corruption from an unset custom coding method .
mem : Consistently return NULL for av_malloc ( 0 )
avidec : add missing initializer braces to shut up gcc warning .
avcodec / vdpau_hevc : Fix potential out - of - bounds write
seek back at the end of file after updating header
avcodec / mlpdec : Do not leave a invalid num_primitive_matrices in the context
Fix a crash when reading gray pam files .
avcodec / magicyuv : set correct size of last slice for each plane
avformat / ivfenc : fix division by zero
lavc / videotoolboxenc : fix open videotoolbox bug on iOS8 . 4
movenc : Check that frag_info entries exist in mov_write_sidx_tag
tcp : Check the return value from getsockopt
get_bits : change the failure condition in init_get_bits
decode : avoid leaks on failure in ff_get_buffer ()
avcodec / flac_parser : Update nb_headers_buffered
avutil / eval : check for av_malloc failure
audio_frame_queue : fix null ptr dereference
oops , potential overflow on really large blocks
avcodec / alsdec : Check for overread
kill uninitialised variable warning in ac3_probe ()
avcodec / cbs_jpeg : Check length for SOS
avformat / movenc : Check that frame_types other than EAC3_FRAME_TYPE_INDEPENDENT have a supported substream id
avformat / avio : fix memory leak in url_find_protocol
avformat / aviobuf : Fix signed integer overflow in avio_seek ()
flashsv : make sure data for zlib priming is available
motionpixels : check extradata size
avformat / oggparseogm : Check available data before reading global header
lavf / img2dec : fix out - of - range check in find_image_range ()
avcodec / h264 : Only reinit quant tables if a new PPS is allowed
rtsp : Free the rtpdec context properly
rawdec : don ' t leak option strings .
Check for out of bound writes when building tree in bink decoder .
ffmpeg : Fix crash with ost -> last_frame allocation failure
avformat / matroskaenc : add missing allocation failure checks for stream durations
vaapi_encode : Discard output buffer if picture submission fails
yopdec : check frame oddness to be within supported limits
fix decoding of coupling leak info for normal AC - 3
avformat / dfa : check first frame offset in probe
avcodec / imc : dont read bits beyond the end
sanity check whether dimensions are non - null
qsvdec : Drop stray extra braces around initializer
avformat / apngenc : use av_packet_alloc ()
avcodec / iff : warn about truncated input to decode_byterun () and clear remaining output
libavcodec / x86 / h264dsp_mmx . c : add forgotten HAVE_YASM
avs : fix infinite loop on end - of - stream .
lavc / libschroedinger : add missing failure checks in ff_create_schro_frame ()
sdp : Fix null pointer dereference with aac and ffserver .
libfdk - aacdec : Correct buffer_size parameter
avcodec / rv40 : Test remaining space in loop of get_dimension ()
crypto : Use av_freep instead of av_free
lavf : fix invalid reads in avformat_find_stream_info ()
mpegvideo : unref buffers in ff_mpeg_unref_picture on frame size changes
eacmv : check for out of bound reads
avcodec / apedec : use av_fast_padded_malloc ()
mpeg12enc : Don ' t set up run - level info for level 0 .
avcodec / dirac_vlc : Fix invalid shift
avcodec / h264_cavlc : Fix undefined behavior on qscale overflow
avformat / utils : Check AVFormatContext -> duration in compute_chapters_end () before use
ac3enc_template : silence may be used uninitialized in this function warnings
avcodec / libtheoraenc : Check for stats allocation failure
lavf / tee : fix leak of select option .
vf_paletteuse : Don ' t free the second frame from ff_framesync_dualinput_get_writable on error
Avoid a null pointer dereference on oom when decoding vc1 .
sws : Fix warning : ‘ abase ’ may be used uninitialized in this function
cook : fix use of uninitialized variable
h264 : reset current_slice on context reinit
avformat / rmdec : Fix Packet memleak at close ()
utils : fix avcodec_flush_buffers pre - reference counting compatibility
avcodec / utils : Clear dimensions in ff_get_buffer () on failure
vf_drawtext : fix memory leak of glyph data .
avcodec / hevc : fix nb_sh / nb_sps check
libavformat : Check for malloc failures in avformat_new_stream
libx265 : Fix ' braces around scalar initializer ' warning
av_probe_input_format3 : support NULL as buffer . Fixes null ptr deref
avformat / utils : fix memory leak in avformat_free_context
sanm : fix undefined behaviour on big - endian .
amrwbdec : Decode the fr_quality bit properly
avcodec / mjpegdec : Add some sanity checks to ljpeg_decode_rgb_scan ()
libtheora : Check frame allocation
avcodec : disallow reget_buffer () if pix_fmt changed .
mmvideo : fix uninitialized variable use in mm_decode_intra
rtsp : Set the real_setup pointer properly , avoid out of bounds memory accesses
avcodec / mpeg4video_parser : Check for extradata parsing failure
avformat / mov : fix memory leak
vc1_parser : fix NULL pointer dereference
ividsp : zero pitch so as not to overread
avformat / utils : Do not select audio streams with unknown sample rate in av_find_best_stream ()
avcodec / mpeg4videodec : use av_fast_padded_malloc () for bitstream buffer
avcodec / huffyuvdec : do not leave alpha values uninitialized
ape : stop reading after the last frame has been read
avformat / utils : fix a codecpar non use
avcodec / sbrdsp_fixed : Fix integer overflow
mov : fix self referencing timecode tracks
avconv : fix off by one check in complex_filter
qsvdec : Fix memory leak
avformat / hls : do not limit manifest lines to 1024 chars
imc : Catch a division by zero
avcodec / diracdec : Fix off by 1 error in quant check
supporting rare overflow mess even with error_resilience >= 0
ffv1 : Make sure at least one slice context is initialized
diracdec : Use only one frame per reference
lavc / options : fix leaks in avcodec_free_context
avformat / librtmp : Fix memory leak if RTMP_ConnectStream () fails
avcodec / sgidec : fix count check
rtsp : Check for command strings without spaces
segafilm : Check for memory allocation failures in segafilm demuxer .
tiff : set the correct return value when check_size () fails
avcodec / eamad : use av_freep (), do not leave stale pointers in memory
avconv : check that the output format context exists before accessing it
avcodec / mjpegdec : Check for end for both bytes in unescaping
h264 : Make sure reinit failures mark the context as not initialized
swscale : fix NULL checking in sws_alloc_context ()
libavutil / opencl : fix potential null dereference
libfdk - aacdec : Fix a boundary check
vsrc_movie : fix leak in request_frame ()
vp8 : prevent read from uninitialized memory in decode_mvs
libavformat / cdxl : fix duration in case of overflow
avformat / mp3dec : Check for avcodec_alloc_context3 () failure
matroska : set " done " only during resync fail .
mpegts : Make sure we don ' t return uninitialized packets
truemotion2 : clear the token array if its initialization fails .
indeo3 : Fix overreading requant_tab .
avcodec / mpegaudioenc_template : dont crash on bitrate = 0
opus_pvq : remove unneeded assert
avcodec / asvdec : Use rounded up dimenensions in input size check
mjpegbdec : dont return a picture when there is no picture .
ffmpeg_cleanup : fix crash with unrecognized codec
zerocodec : check if there is previous frame
elsdec : Replace EOVERFLOW with INVALIDDATA
avcodec / nuv : zero buffer padding
wmavoice : fix a signed overflow
avcodec / webp : Fixes null pointer dereference
mxfdec : free descriptor extradata in . read_close ()
oggparseogm : check timing variables
lavfi / asyncts : Fix compilation with GCC 4 . 7
ffmpeg : Dont crash on unconnected output pads before an error is printed
ttadec : check channel count as read from extradata .
vaapi : return early from ff_vaapi_render_picture () without picture
vf_format : check input validity
avcodec / jpeg2000dwt : Fix integer overflow in dwt_decode97_int ()
mm : Skip unexpected audio packets
hls : fix leaking avio_opts on hls_read_header error
avcodec / mpegvideoenc : fix undefined negative left shift
avformat / adxdec : check avctx -> channels for invalid values
crc : fix signed overflow
avcodec / vp56 : Reset have_undamaged_frame on resolution changes
lavc / h264 : move ff_init_cabac_states () from decode_slice () to ff_h264_decode_init ().
mov : fix global unicode convertion array overflow .
imc : add required padding for GetBitContext buffer
idcin : fix memleak
Free the RTSPStreams in ff_rtsp_close_streams
avcodec / pafvideo : Fix assertion failure
mxfdec : fix potential integer overflow in mxf_compute_sample_count ()
vsrc_movie : fix leak in request_frame ()
check for error
lavd / v4l2 : do not fail when VIDIOC_ENUMSTD returns ENODATA
avcodec / dnxhddec : reset cur_field for non interlaced frames
lavfi / ocv : fix crash with no filter name specified .
h264 : only assign H264Picture :: mbaff for first slice .
vc2enc : fix segfault
avformat / oggenc : check for stream private data in ogg_free ()
avcodec / ffv1dec : update progress in case of broken pointer chains
avcodec / hevcdsp_template . c : Fix undefined shift in FUNC ( dequant )
avcodec / aacdec_fixed : Fix runtime error : left shift of negative value - 1297616
cmdutils_opencl : Fix read of uninitialized pointer
lavf : revert commit bb99ae3ae924c942a634bec7711ec7ee11c38eb9
check for qscale == 0 ( fixes 1 / 0 on one corrupted stream )
avcodec / hevc_parser : check for av_mallocz () failure
swscale : Do not expand a macro with ' defined ' in it
oggdec : fix invalid free on error
smvjpegdec : make sure cur_frame is not negative
movenc : fixes a questionable valgrind uninitialized value warning
avcodec / dxva2_h264 : Fix " may be used uninitialized " warnings
swr : fix " may be used uninitialized in this function " warnings
mpeg12 : terminate mpeg2_video_profiles arrays
opus_pvq : prevent division by 0
msmpeg4v1 : fix undefined behaviour in msmpeg4_decode_picture_header ()
avcodec / webp : Add missing input padding
vsrc_testsrc : fix off - by - one logic when detecting the EOF time in request_frame ()
avformat / matroskadec : fix resource leak
aacdec : free frame buffer if no audio was decoded
avformat / sbgdec : Check alloc_array_elem () return value
checkasm : Give macro a body to avoid potential unexpected syntax issues
ffmpeg_opt : Check for localtime () failure
libavcodec : Don ' t do av_free ( av_malloc ( 0 )) for bitstream filters
remove useless cast that makes code unreadable
avidec : update size when packet is shrunk
png : Use av_freep () instead of av_free ()
vc1 : properly zero coded_block [] edges on new slice entry .
avpacket : Initialize the allocated padding area in side data
vf_overlay_opencl : Don ' t leak output frame on error
avcodec / wavpack : Fix invalid shift and integer overflow
id3v2 : catch avio_read errors in check_tag
VP8 : add missing free
cfhd : fix off - by - one error in level check
libschroedingerdec : don ' t produce empty frames
mov : fix heap buffer overflow
g723 . 1 : fix addition overflow
asf : only set index_read if the index contained entries .
avcodec / aac_adtstoasc_bsf : Fix memory leak
avcodec / vp9 : mark frame as finished on decode_tiles () failure
avcodec / h264 : Be more strict on rejecting pps_id changes
mmvideo : restore initial y value .
avcodec / dxv : Check for end of input in dxv_decompress_dxt5 ()
j2k : reorder multiplications to avoid interger overflows related to precincts
riff : don ' t overwrite bps from WAVEFORMATEX if EXTENSIBLE doesn ' t contain that data .
examples / filtering_audio : fix frame leak .
check std tag size before reading .
qsvdec : Fix memory leak on error
ra144enc : fix two " may be used uninitialized in this function " warnings
avformat / asfdec_f : Fix memleak
avcodec / wmaprodec : check that there are input bits left in decode_subframe_length ()
avcodec / ratecontrol : silence " warning : expected_bits may be used uninitialized in this function "
avformat / utils : Check cur_dts in update_initial_timestamps () more
applehttp : Change the variable for stream position in seconds into int64_t
avutil / fixed_dsp : Check for av_malloc () failure
avformat / oggparseopus : Check that granule pos is within the supported range
tests / api / api - codec - param - test : Do not directly access caps_internal
mpc8 : Check out of bound bands limit
AMV : disable DR1 and don ' t override EMU_EDGE
h264 : cap max has_b_frames at MAX_DELAYED_PIC_COUNT - 1 .
ac3 : validate end in ff_ac3_bit_alloc_calc_mask
smacker : Avoid integer overflow when allocating packets
doc / examples : Handle new types in avio_list_dir
matroskadec : check h in generic rm packet shuffler
avformat / avidec : set average frame rate from header
cavsdec : check dimensions being valid .
wtv : Make WTV_SECTOR_BITS a 64 bit constant
http : Don ' t use the normal http proxy mechanism for https
binkaudio : prevent extradata overread
lavf / find_stream_info : make sure we decode at least one frame for DTS
avcodec / nvenc : Fix forcing constqp rc mode
avpacket : fix size check in packet_alloc
avcodec / mpegaudioenc_template : Fix integer overflow
avcodec / rangecoder : Fix range coder corner case handling
av_free () -> av_freep (), patch by Reimar Doffinger , see discussion in
lavd / xv : free graphics context
avformat / asfenc : Check for index_ptr allocation failure
af_amix : avoid spurious EAGAIN .
avformat / concatdec : avoid NULL dereference when failed to open file .
avcodec / hevc : check slice_header_extension data length
matroskadec : fix NULL pointer dereference in webm_dash_manifest_read_header
h264 : return the consumed amountg in case of Q264
mpegvideo : synchronize AVFrame pointers in ERContext fully
avcodec / exr : fix undefined shift in pxr24_uncompress ()
vp3 : Copy all 3 frames for thread updates .
libxvid : Clear extradata pointer when freeing it .
avcodec / mjpegdec : sanity check bits
xmv : Fix integer overflow
avcodec / mpeg4videodec : update bug workaround related variables like they where before
incorrect memcpy size ; adobriyan @ gmail . com
cast vararg sentinel value to pointer
Return early from NVS command submission if no response is required
bail out early , skip snprintf , if buflen is 0 ; ok cloder
plug a mem leak
careful with mmap off
trace : break circular dependency in event - internal . h
kvm - irqchip : only commit route when irqchip is used
parallels : wrong call to bdrv_truncate
qcow2 : Assert against currently impossible overflow
print meaningful error message in case of -- disable - vhost - net
vhost : check for vhost_ops before using .
qed : Refuse to create images on block devices
qapi : Fix memory leak
hw / intc / slavio_intctl : Avoid shifting left into sign bit
tests : Run the luks tests in test - crypto - block only if encryption is available
usb - serial : Fail instead of crash when chardev is missing
pseries : Don ' t allow TCE ( iommu ) tables to be registered with duplicate LIOBNs
target - sparc : on UA2005 don ' t deliver Interrupt_level_n IRQs in hypervisor mode
target / sh4 : fix TCG leak during gusa sequence
input : add sanity check
block : Plug memory leak on brv_open_image () error path
tests : test - qmp - commands : Fix double free
block : Detect multiplication overflow in bdrv_getlength
ide : fix halted IO segfault at reset
tests : fix virtio - scsi - test leak
vvfat : initialize memory after allocating it
qemu - img : Fix insignificant memleak
exec : fix TB invalidation after breakpoint insertion / deletion
qdev : fix OVERFLOW_BEFORE_WIDEN
target - i386 : Make x86 mfence and lfence illegal without SSE2
sparc / leon3 . c : fix memory leak
hw / ppc / e500 . c : Fix memory leak
pulse - audio : fix bug on updating rpos
ehci : clear suspend bit on detach
cirrus : Fix host CPU blits
hostmem - file : plug a small leak
vhost : migration blocker only if shared log is used
virtio : avoid buffer overrun on incoming migration
block / iscsi : fix uninitialized variable
bootdevice : add check in restore_boot_order ()
vmdk : Fix integer overflow in offset calculation
net : Avoid NULL function pointer dereference on cleanup
vmxnet3 : Do not fill stats if device is inactive
l2tpv3 : fix possible double free
target - s390x : Only access allocated storage keys
fix : unrealize virtio device if we fail to hotplug it
hw / xen_disk : aio_inflight not released in handling ioreq when nr_segments == 0
s390x : fix KVM target
Fix a crash with monitor input arriving before readline_start has been called
esp : cancel current request only if some request is in flight
qemu - img : Plug memory leak on block option help error path
block / raw - posix : Don ' t think / dev / fd /< NN > is a floppy drive .
esp - pci : fixup deadlock with linux
vl : new runstate transition : RUN_STATE_GUEST_PANICKED -> RUN_STATE_FINISH_MIGRATE
hw / arm / highbank : Fix resource leak and wrong image loading
lm32 : avoid buffer overrun
target - mips : fix EntryHi . EHINV being cleared on TLB exception
block : Fix memory leak in hmp_drive_add_node ()
linux - user / sh4 : fix incorrect memory write
dma : Fix stupid typo / thinko
macio : set res_count value to 0 after non - block ATAPI DMA transfers
sdhci : don ' t raise a command index error for an unexpected response
blockdev : use drained_begin / end for qmp_block_resize
qcow2 : Avoid integer overflow in get_refcount ( CVE - 2014 - 0143 )
vnc : fix a memory leak in threaded vnc server
hw / ptimer : Do not artificially limit timers when using icount
vhost - user : unregister slave req handler at cleanup time
io : fix mem leak in websock error path
qom : Fix crash with qom - list and link properties
qcow2 : fix null pointer dereference
virtio : Plug memleak by freeing vdev
hw / 9pfs : Be robust against paths without FS_IOC_GETVERSION
darwin - user : Remove unneeded null pointer check
linux - user : make bogus negative iovec lengths fail EINVAL
init_paths : fix minor memory leak
sparc32 fix np dereference in do_unassigned_access
pci_bridge : use parent bus ' s address space
block / vmdk : Fix the endian problem of buf_len and lba
qemu - socket : don ' t leak opts on error
virtio : Remove malloc failure checks ( Jan Kiszka )
spapr_vscsi : fix build error introduced by f19661c8
Avoid a build warning .
macio : fix overflow in lba to offset conversion for ATAPI devices
qcow2 : Point to amend function in check
block / qapi : Plug memory leak in dump_qobject () case QTYPE_QERROR
block : fix qcow2_co_flush deadlock
vnc : fix segfault
hw / arm : Mark the " fsl , imx31 " device with user_creatable = false
clang : Fix warning reg . expansion to ' defined '
vdi : Fix memory leak
target - ppc : free a tcg temp variable
arm / translate - a64 : mark path as unreachable to eliminate warning
tests : fix e1000 - test leak
qcow2 : Fix corruption after error in update_refcount
target - alpha : Suppress underflow from CVTTQ if DNZ
isapc : Fix irq routing
hw / openrisc : Avoid undefined shift in openrisc_pic_cpu_handler ()
qcow2 : Remove abort on free_clusters failure
ide : Correct improper smart self test counter reset in ide core .
tcg - sparc : Dump illegal opode contents
hmp : fix qemu crash due to ioapic state dump w / split irqchip
tcg : Fix off - by - one in assert in page_set_flags
qxl : Slot sanity check in qxl_phys2virt () is off by one , fix
block / qcow2 : Add missing GCC_FMT_ATTR to function report_unsupported ()
block : Fix harmless off - by - one in bdrv_aligned_preadv ()
replay : Fix dangling location bug in replay_configure ()
hw / display / exynos4210_fimd : Fix bit - swapping code
watchdog : fix deadlock with - watchdog - action pause
ahci : clear aiocb in ncq_cb
qcow2 : Use BB for resizing in qcow2_amend_options ()
sdl : remove NULL check , g_malloc0 can ' t fail
tcg / ppc64 : Fix zero extension code generation bug for ppc64 host
usb : fix use after free
vnc : fix a qio - channel leak
target / ppc : Fix setting of cpu -> compat_pvr on incoming migration
hw / arm / digic : Mark device with user_creatable = false
pcihp : fix possible array out of bounds
target - i386 : key sfence availability on CPUID_SSE , not CPUID_SSE2
qga : don ' t fail if mount doesn ' t have slave devices
pseries : Check we have a chardev in spapr_vty_init ()
tests : fix bios - tables - test leak
ahci : advertise HOST_CAP_64
hmp : migrate command ( without - d ) now blocks correctly
block : Fix bdrv_is_allocated () return value
linux - user : fix do_rt_sigreturn on m68k linux userspace emulation
linux - user / signal . c : PPC : Silence clang uninitialized - use warning
ppc : Remove a potential overflow in muldiv64 ()
usb / ohci : Fix crash with when specifying too many num - ports
ppc : avoid buffer overrun : use pstrcpy , not strncpy
block : record new size in bdrv_dirty_bitmap_truncate
keep the PID file locked for the lifetime of the process
net : avoid infinite loop when receiving packets ( CVE - 2015 - 5278 )
spapr_rng : disable hotpluggability
qxl : switch display on entering VGA
iohandler : Use bool for boolean struct member and remove holes
vfio / common : cleanup in vfio_region_finalize
qemu - io : Fix memory leak in ' aio_write - z '
hw / i386 / acpi_build . c : Avoid shifting left into sign bit
cuda . c : return error for unknown commands
char : remove use - after - free on win - stdio
microblaze : boot : Don ' t hack the elf entry point
usb - ohci : Fix memory leak for ohci timer
linux - user / signal . c : Correct error path for AArch64 do_rt_sigreturn
qemu - ga : ga_open_pidfile (): use qemu_open ()
vhost : remove an incorrect assert
qcow2 : Prevent numerical overflow
qemu - thread : do not use PTHREAD_MUTEX_ERRORCHECK
ppc : fix monitor access to CR
virtio - net : count VIRTIO_NET_F_MAC when calculating config_len
i8254 : fix out - of - bounds memory access in pit_ioport_read ()
replay : exception replay fix
target / loongarch : Clean up tlb when cpu reset
hw / display / cg3 . c : Fix memory leak
lm32 : fix build breakage due to uninitialized variable ' r '
vfio - pci : Fix missing unparent of dynamically allocated MemoryRegion
e500 : fix memory leak
linux - user : return EINVAL from prctl ( PR_ * _SECCOMP )
xilinx_timer : Fixed deadlock issue
slirp : fix segv when init failed
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
ipmi_bmc_sim : Fix off by one in check .
vnc : fix coverity warning
vnc : fix qemu crash when not configure vnc option
s390x : s390_cpu_get_phys_page_debug has to return - 1
vnc : clear vs -> tlscreds after unparenting it
nand : fix address overflow
tap : clear vhost_net backend on cleanup
target - s390x : Add missing tcg_temp_free_i64 () in disas_s390_insn (), opc == 0x8e
linux - user : In fork_end (), remove correct CPUs from CPU list
rng random backend : check for - EAGAIN errors on read
tests : fix eepro100 - test leak
target - ppc : fix a TCG local variable creation
coroutine : Fix use after free with qemu_coroutine_yield ()
tcg : allocate s -> op_dead_iargs dynamically
block / vvfat : Plug memory leak in enable_write_target ()
vhost / scsi : call vhost_dev_cleanup () at unrealize () time
hw / nvram / spapr_nvram : Device can not be created by the users
xen - platform : Ensure xen is enabled when initializing
rtl8139 : skip offload on short TCP header ( CVE - 2015 - 5165 )
spapr : Fix undefined behaviour in spapr_tce_reset ()
usb : sanity check setup_index + setup_len in post_load
cpus . c : qemu_mutex_lock_iothread fix race condition at cpu thread init
fix QemuOpts leak
cuda : fix crash on Windows
suppress a couple of spurious warnings in scsi - generic . c
scsi : Make device scsi - disk reject / dev / sg *
qdev : Fix crash on - device '?= x '
vnc : fix crash when vnc_server_info_get has an error
audio : release capture buffers
tests : fix q35 - test leaks
scsi : megasas : initialise local configuration data buffer
qom : Make object_property_set_qobject ()' s input visitor strict
tci : don ' t write zero for reloc in tci_out_label
s390x : Limit s390 - ccw machines to 248 CPUs
qcow2 : Avoid overflow in alloc_clusters_noref ()
block / drive - mirror : Reuse backing HD for sync = none
hw / scsi - bus . c : Fix use of uninitialised variable
hid : Reset kbd modifiers on reset
qdev : Fix use after free in qdev_init_nofail error path
qemu - char : Fix crash when switching consoles
linux - user - i386 : Fix crash on cpuid
tests : fix check - qobject leak
rtl8139 : honor RxOverflow flag in can_receive method
vpc : Handle failure for potentially large allocations
pci : deassert intx when pci device unrealize
char : serial : check divider value against baud base
[ virtio - 9p ] open should not return EBADF
crypto : fix leak in ivgen essiv init
Bugfix : PowerPC 64 slbia never invalidates the first segment entry .
qcow2 : Fix leaks in dirty images
libqos : Correct memory leak
qapi : Avoid output visitor crashing if it encounters a NULL value
vvfat : fix qemu - img map and qemu - img convert
hw / pc_sysfw : Fix memory leak
qemu - sockets : Plug fd leak on unix_connect_opts () error path
hw / 9pfs / virtio - 9p - posix - acl : Fix out - of - bounds access
target / arm : fix TCG temp leak in aarch64 rev16
qemu - img bench : Fix uninitialised writethrough mode
block / nbd : fix segmentation fault when . desc is not null - terminated
rtl8139 : Do not consume the packet during overflow in standard mode .
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
scsi : initialise info object with appropriate size
exec - all . h : Use stl_p to avoid undefined behaviour patching x86 jumps
block : Reset buffer alignment on detach
s390x / mmu : Fix the exception codes for illegal table entries
target - arm : Stage 2 permission fault was fixed in AArch32 state
usb : fix usb - net segfault
blockdev : unset inappropriate flags when changing medium
vhost : assert the log was cleaned up
hw / 9pfs : Fix potential memory leak and avoid reuse of freed memory
target - i386 : cpu : Fix potential buffer overrun in get_register_name_32 ()
x86 : translate . c : remove dead assignment
ide : Check validity of logical block size
vnc : fix qemu crash because of SIGSEGV
qemu / slirp : Fix SMB security configuration on newer samba versions
tcg / mips : Fix regression caused by typo ( copy + paste bug )
ahci : fix buffer overrun on invalid state load
use new helper name
acpi - build : fix misaligned access
xen - pt : fix Out - of - bounds read
ehci : add sanity check for maxframes
tpm : Fix write to file descriptor function
ps2 : reset queue in ps2_reset_keyboard
exec : fix breakpoint_invalidate when pc may not be translated
target - s390x : fix CC computation for EX instruction
xen_disk : fix possible null - ptr dereference
ahci : free irqs array
pcie_aer : clear cmask for Advanced Error Interrupt Message Number
target / m68k : fix TCG variable double free
x86 : ioapic : fix fail migration when irqchip = split
tests : Fix two memory leaks
qcow2 : Prevent allocating compressed clusters at offset 0
misc : zynq - xadc : Fix off - by - one
ppc : Let kvmppc_reset_htab () return 0 for ! CONFIG_KVM
target - ppc : Fix Temporary Variable Leak in bctar
qdev - monitor : Fix crash when device_add is called with abstract driver
qdev : Fix crash by validating the object type
fix memory leak in aio_write_f
target - arm : Signal Underflow when denormal flushed to zero on output
qapi / block : Add " fatal " to BLOCK_IMAGE_CORRUPTED
qcow2 : Fix error handling in qcow2_alloc_cluster_offset
usb - storage : fix possible memory leak and missing error message
qcow2 : Put cache reference in error case
virtio : zero vq -> inuse in virtio_reset ()
s390x / kvm : fix small race reboot vs . cmma
slirp : check len against dhcp options array end
target - s390x : fix CC computation for LOAD POSITIVE instructions
gtk : skip keyboard grab when hover autograb is active
slirp : tftp , copy sockaddr_size
target - i386 : Fix x86_cpuid_set_model_id ()
s390x / cpu : use g_new0
target - arm : Make arm_current_el () return sensible values for M profile
tcg : Fix compiler error ( comparison of unsigned expression )
bitops : fix test_and_change_bit ()
dt : temporarily disable subtree creation failure check
hw / arm / aspeed_soc : Mark devices as user_creatable = false
exec . c : Fix off - by - one error in register_subpage
qcow2 : Fix header extension size check
linux - user : pause () should not pause if signal pending
hw / misc / ivshmem : Fix ivshmem_recv_msg () to also work on big endian systems
scsi : esp : check buffer length before reading scsi command
s390x / sclp : mark sclp - cpu - hotplug as non - usercreatable
vhost : fix double free on device stop
qga : fix ' driver ' leak in guest - get - fsinfo
tcg / optimize : optimize " op r , a , a => mov r , a "
qxl : locking fix
qemu - thread : fix qemu_event without futexes
json - streamer : Don ' t leak tokens on incomplete parse
kvm : Switch to unlocked MMIO
vhost - user : check qemu_chr_fe_set_msgfds () return value
stream : fix crash in stream_start () when block_job_create () fails
virtio : decrement vq -> inuse in virtqueue_discard ()
vl : free err
scsi : Specify the xfer direction for UNMAP and ATA_PASSTHROUGH commands
io : fix qio_channel_socket_accept err handling
ppc / xics : An ICS with offset 0 is assumed to be uninitialized
hw / arm / virt : fix cpu object reference leak
spapr : fix off - by - one error in spapr_ovec_populate_dt ()
block : better error message for read only format name
iscsi : Handle failure for potentially large allocations
block : Limit size to INT_MAX in bdrv_check_byte_request ()
spapr_drc : Expose ' null ' in qom - get when there is no fdt
block : Limit request size ( CVE - 2014 - 0143 )
console : fix - vga none - sdl crash
i386 : define the AMD ' amd - ssbd ' CPUID feature bit
QMP : Check " arguments " member ' s type
AHCI : Fix port reset race
hw / pl011 . c : Avoid crash on read when no chr backend present
target - ppc / fpu_helper : Fix efscmp * instructions handling
block / vpc : fix uninitialised variable compiler warning
block - verify : fix 32 - bit build
target - i386 : svm : Fix MSRPM check
blkverify : Fix BDS leak in . bdrv_open error path
spice : fix memory leak
powerpc / kvm : Fix a uninitialized bug ( Liu Yu )
ui : fix dcl unregister
pcnet : fix rx buffer overflow ( CVE - 2015 - 7512 )
crypto : assert that qcrypto_hash_digest_len is in range
drive mirror : fix memory leak
acpi : Assert sts array limit on AcpiCpuHotplug_add ()
block : clean up bdrv_drain_all () throttling comments
syscall : fix use of uninitialized values
ide : Remove unneeded null pointer check
qcow2 : Fix memory leak in COW error path
ppc : fix crash in ppc system single step support
qjson : Apply nesting limit more sanely
kvm : de - register mem region for MMIO ( Glauber Costa )
pci : fix pci_info_device ().
kvm : add error message for when SMP is requested
hmp : fix memory leak at hmp_info_block_jobs ()
input : Add trace event for empty keyboard queue
hw / vfio / platform : replace g_malloc0_n by g_new0
vhost : fix mem_sections memory corruption
qcow2 : Check backing_file_offset ( CVE - 2014 - 0144 )
qom : fix leak for objects created with - object
acpi - build : fix memory leak with bridge hp off
spapr_drc : Make device " spapr - dr - connector " unavailable with - device
pci / shpc : fix signed integer overflow
throttle : Set always an average value when setting a maximum value
slirp : Check qemu_socket () return value in udp_listen ()
kvm : Fix potential resource leak ( missing fclose )
linux - user : Fix invalid TARGET_ABI_BITS usage on ppc hosts
cadence_uart : bounds check write offset
tests : fix check - qom - interface leaks
acpi : avoid potential uninitialized access to cpu_hp_io_base
vhost - user : verify that number of queues is non - zero
target / arm : Fix 32 - bit address truncation
fdc - test : insert media before fuzzing registers
vhost : check region type before casting
error : Copy location information in error_copy ()
qemu - ga : Plug fd leak on ga_channel_open () error paths
chardev : use backend chr context when watch for fe
qdev : Free opts on failed do_device_add
xen_pt : Present the size of 64 bit BARs correctly
rdma : fix memory leak
usb : Fix usb_ep_find_packet_by_id
qemu - img : Fix memory leak
qdev : Fix qdev_try_create () semantics
block : Fix multiwrite error handling
loader : fix undefined behavior in rom_order_compare ()
vmdk : Fix use of uninitialised value
gdbstub : do not restart crashed guest
armv7m : R14 should reset to 0xffffffff
vl : fix memory leak spotted by valgrind
qlist : Make conversion from QObject * accept null
qemu - img : Fix leakage of options on error
hw / pci / pci_host . c : Avoid shifting left into sign bit
block : Remove redundant assertion
target - i386 / kvm . c : Code formatting changes
qcow2 : Free allocated snapshot table on error
Check the returned audio_buf_info fields
spapr : fix possible memory leak
xenfb : use the correct condition to avoid excessive looping
block : Fix serial number assignment
hw / vmmouse . c : Disable vmmouse after reboot
virtio - 9p - proxy : Fix sockfd leak
dcbz fix ( Jocelyn Mayer )
check - qdict : Fix possible crash
Initialize a variable in all cases
spapr / htab : fix savevm
hw / display / tcx . c : Fix memory leak
spapr : support only ELF kernel images
block / vvfat : Plug memory leak in read_directory ()
vhost : fix dirty page handling
qemu - io : fix a memory leak
sockets : Plug memory leak in socket_address_flatten ()
tcg : correctly mark dead inputs for mov with a constant
qemu - timer . c : Trim list of included headers
target - i386 : Fix apic object leak when CPU is deleted
replay : bh scheduling fix
s390x / tcg : handle WAIT PSWs during interrupt injection
timer : stm32f2xx_timer : add check for prescaler value
qom : Fix memory leak in object_property_set_link ()
spapr_pci : parent the MSI memory region to the PHB
ppc64 : fix compressed dump with pseries kernel
vnc : fix incorrect checking condition when updating client
ppc405 : Fix memory leak
throttle : Remove block from group on hot - unplug
qga : fix uninitialized value warning for win32
tap : clear vhost_net backend on cleanup
spapr : initialize local Error pointer
aio - posix : honor is_external in AioContext polling
block / drive - mirror : Check for NULL backing_hd
slirp : fix structure initialization in tcp_listen ()
mptsas : fix memory leak
crypto : fix stack - buffer - overflow error
linux - user : getpriority errno fix
block / nbd : Fix the leaked visitor
io : fix check for handshake completion in TLS test
qga : fix false negative argument passing
gtk : fix uninitialized temporary VirtualConsole
win32 - aio : Fix memory leak
s390x / css : start with cleared cstat / dstat
PPC : Fix TLB invalidation bug within the PPC interrupt handler .
pci : fix double free of romfile property
scsi : Add assertion for use - after - free errors
parallels : fix default options parsing
dirty - bitmap : Report BlockDirtyInfo . count in bytes , as documented
audio : Don ' t free hw resources until after hw backend is stopped
blockdev : Plug memory leak in drive_uninit ()
hw / intc / exynos4210_combiner : Don ' t overrun output_irq array in init
sheepdog : fix resource leak with sd_snapshot_create
xen / pt : Set is_express to avoid out - of - bounds write
bitmap . h : Don ' t include qemu - common . h
block : Catch ! bs -> drv in bdrv_check ()
disas / i386 : Avoid NULL pointer dereference in error case
qemu - ga : Plug file descriptor leak on ga_open_pidfile () error path
qga : fix off - by - one length check
vl : Tighten parsing of - numa ' s parameter mem
console : add same surface replace pre - condition
pci : memory leak of PCIDevice :: rom_file
vmxnet3 : validate queues configuration read on migration
ide : only constrain read / write requests to drive size , not other types
tests : fix leak in test - string - input - visitor
hw / virtio - 9p - local . c : Remove unnecessary null char in symlink file
spice : abort on invalid streaming cmdline params
ide : refuse WIN_READ_NATIVE_MAX on empty device
ui / curses . c : Ensure we don ' t read off the end of curses2qemu array
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
json - parser : Fix potential NULL pointer segfault
qapi : Fix c_name () munging
thread - pool : avoid deadlock in nested aio_poll () calls
linux - user / syscall . c : lock_iovec : unlock vec [ i ] in failure processing code block
json - parser : add handling for NULL token list
cpus : initialize cpu -> memory_dispatch
block : Fix multiwrite memory leak in error case
linux - user : fix file descriptor leaks
tcg / ppc : Do not overwrite lower address word on Darwin and AIX
input : Decrement queue count on kbd delay
ARM : fix segfault
vvfat : fix a file descriptor leak
hmp : fix crash in ' info block - n - v '
rtc - test : skip year - 2038 overflow check in case time_t is 32bit only
vmdk : Fix overflow if l1_size is 0x20000000
sdl : Do not make full screen mode resizable
target - i386 : Fix mulx for identical target regs
linux - user : zero fstat buffer to initialize nsec fields
slirp : fix CVE 2007 - 5729
qxl : clear irq on reset
ccid - card - emul : do not crash if backend is not provided
kvm / ioapic : dump real object instead of a fake one
linux - user : remove ifdef __USER_MISC
qemu / iovec : Don ' t assert if sbytes is zero
linux - user : Correct type for BLKSSZGET
virtio - net : fix guest - triggerable buffer overrun
vfio / ccw : allocate irq info with the right size
fix race between timer firing vs . alarm_timer -> pending = 0
fix bug in block - qcow2 . c : alloc_cluster_offset () ( Shahar Frank )
qcow2 : Fix L1 allocation size in qcow2_snapshot_load_tmp () ( CVE - 2014 - 0145 )
qemu - ga : for w32 , fix leaked handle ov . hEvent in ga_channel_write ()
cadence_uart : Fix buffer overflow
usb - redir : Fix crash on migration with no client connected
qxl : Fix new function name for spice - server library
qcow2 : Remove dead check on ! ret
qemu - common : add QEMU_ALIGN_DOWN () and QEMU_ALIGN_UP () macros
block / ssh : Avoid segfault if inet_connect doesn ' t set errno .
usb : fix up post load checks
hw / arm / virt : Fix corruption due to double free
gdbstub : fix for commit 87f25c12bfeaaa0c41fb857713bbc7e8a9b757dc
block : Fix bdrv_all_delete_snapshot () error handling
kvm : Add kvm system event crash handler
x86 / cpuid : Plug memory leak in cpudef_setfield ()
s390 : fix reset hypercall to reset the status
usb : sanity check setup_index + setup_len in post_load
xhci : fix overflow in usb_xhci_post_load
linux - aio : Fix laio resource leak
ppc : fix double - free in cpu_post_load ()
qapi / string - output - visitor : fix human output
qemu / migration : fix the double free problem on from_src_file
hw / arm / virt : Check that the CPU realize method succeeded
qemu - timer : fix off - by - one
tests : fix rsp leak in postcopy - test
pc - dimm : error out if memory hotplug is not enabled
ivshmem - server : fix possible OVERRUN
ui : fix crash with sendkey and raw key numbers
ivshmem : fix memory_region_del_eventfd assertion failure
s390x : silence warning from GCC on uninitialized values
char : fix missing return in error path for chardev TLS init
qemu - char : qemu_chr_open_fd () can ' t fail , don ' t check
mirror : Fix resource leak when bdrv_getlength fails
target - s390x : Mark check_privileged () as ! CONFIG_USER_ONLY
hw / core / loader : Fix possible crash in rom_copy ()
vnc : fix segfault in vnc_display_pw_expire ()
qemu - config : fix leak in query - command - line - options
acpi : add dummy write function for acpi timer
virtio : increase the queue limit to 1024
watchdog / wdt_diag288 : Mark diag288 watchdog as non - hotpluggable
ossaudio : fix memory leak
pci - devfn : check that device / slot number is within range
target - alpha : fix uninitialized variable
vhost - scsi : fix k -> set_guest_notifiers () NULL dereference
pseries : Correct panic behaviour for pseries machine type
flush TLBs at cpu reset
qdev : fix use - after - free in the error path of qdev_init_nofail
tests : fix memory leak in virtio - scsi - test
container : don ' t leak container reference
chardev / baum : fix baum that releases brlapi twice
qcow2 : fix condition in is_zero_cluster
fix return check for KVM_GET_DIRTY_LOG ioctl
slirp : fix use - after - free
hw / ppc / spapr_iommu : Fix crash when removing the " spapr - tce - table " device
vvfat : Fix compilation with DEBUG defined
page_cache : fix memory leak
vga : fix invalid read after free
block / vvfat : Plug memory leak in check_directory_consistency ()
runstate / migrate : Two more transitions
mempath prefault : fix off - by - one error
qcow2 : Fix sector calculation in qcow2_measure ()
xio3130_downstream : Report error if pcie_chassis_add_slot () failed
usb : xhci : fix memory leak in usb_xhci_exit
linux - aio : Fix laio_submit error handling
usb : free leaking path
hw / usb / bus : Remove bad object_unparent () from usb_try_create_simple ()
qemu - socket : set passed fd non - blocking in socket_connect ()
isapc : fix segfault .
qcow2 : Don ' t open images with header . refcount_table_clusters == 0
hw / intc / arm_gicv3_common : Give no - migration - shift - bug subsection a needed function
target - mips : Fix type cast for w64 ( uintptr_t )
pci : fix corrupted pci conf index register by unaligned write
block / gluster . c : Handle qdict_array_entries () failure
virtio : correctly initialize vm_running
qcow2_check : Mark image consistent
qxl : call dpy_gfx_resize when entering vga mode
usb : ehci : fix memory leak in ehci_init_transfer
target - arm / translate - a64 : Fix wrong mmu_idx usage for LDT / STT
qom : Fix memory leak in function container_get
scsi : fix warning
block : Assert validity of BdrvActionOps
vnc : Fix fatal crash with vnc reverse mode
target - i386 : fix Coverity complaints about overflows
block / raw - posix : Fix disk corruption in try_fiemap
usb : xhci : no DMA on HC reset
target - arm : Correct " preferred return address " for cpreg access exceptions
hw / ppc / spapr_rtc : Mark the RTC device with user_creatable = false
raw_bsd : don ' t check size alignment when only offset is set
hw / 9pfs : Fix crash when mounting with synthfs
spapr : fix memory leak in spapr_memory_pre_plug ()
aio - win32 : fix uninitialized use of have_select_revents
mirror : Handle failure for potentially large allocations
ppc : Fix bug in handling of PAPR hypercall exits
qdev : Fix memory leak in function set_pci_devfn
sdl2 : fix crash in handle_windowevent () when restoring the screen size
usb - redir : fix use - after - free
target - i386 : Use 1UL for bit shift
usb : check USB configuration descriptor object
kvm : Mark VCPU state dirty on creation
vmdk : Fix big flat extent IO
block / mirror : check backing in bdrv_mirror_top_refresh_filename
pty : Fix byte loss bug when connecting to pty
block / raw - posix : Open file descriptor O_RDWR to work around glibc posix_fallocate emulation issue .
slirp : udp : fix NULL pointer dereference because of uninitialized socket
pc_sysfw : Check for qemu_find_file () failure
vvfat : Fix potential buffer overflow
target - ppc : KVMPPC_H_CAS fix cpu - version endianess
pc : memory : Validate alignment of maxram_size to page size
target / ppc : fix cpu_ov setting for 32 - bit
block / vpc : make checks on max table size a bit more lax
slirp : Fix memory leak on small incoming ipv4 packet
vfio - pci : Further fix BAR size overflow
qcow2 : Fix potential qemu - img check crash on 32 bit hosts
vmdk : Widen before shifting 32 bit header field
scsi - bus : Fix transfer length for VERIFY with BYTCHK = 11b
hw / ppc / spapr_cpu_core : Add a proper check for spapr machine
qdev - properties . h : Explicitly set the default value for arraylen properties
qcow2 : fix leak of Qcow2DiscardRegion in update_refcount_discard
tests : fix coroutine leak in / basic / entered
exec : fix ram_list dirty map optimization
block / iscsi : fix memory corruption on iscsi resize
hw / intc / xilinx_intc : Avoid shifting left into sign bit
hw / mips_malta : Fix YAMON API print routine
sheepdog : Fix crash in co_read_response ()
linux - user : avoid gcc array overrun warning for sparc
qemu - img : fix failed autotests
block : Catch integer overflow in bdrv_rw_co ()
fix build failure in nbd_read_reply_entry ()
usb - storage : fix NULL pointer dereference .
fix double free the memslot in kvm_set_phys_mem
qxl : stop dirty loging when not in vga mode
s390 - ccw : Fix alignment for CCW1
qxl : simplify qxl_rom_size
qemu - char : fix infinite recursion connecting to monitor pty
target - arm : fix for exponent comparison in recpe_f64
input : fix memory leak
s390x : fix cpu object referrence leak in s390x_new_cpu ()
pc_sysfw : Fix ISA BIOS init for ridiculously big flash
target - i386 : avoid overflow in the tsc - frequency property
qcow2 : remove memory leak
rtl8139 : skip offload on short Ethernet / IP header ( CVE - 2015 - 5165 )
block : avoid useless checks on acb -> bh
mirror : make sectors_in_flight int64_t
kvmvapic : Disable if there is insufficient memory
xlnx - ep108 : Fix minimum RAM check
spapr : fix leak in h_client_architecture_support ()
e1000 : flush packets when link comes up
qemu - char : don ' t leak opts on error
QOM : Fail casts for unknown types
ccid : Fix buffer overrun in handling of VSC_ATR message
fsdev : fix virtfs - proxy - helper cwd
qemu - img : Fix crash in amend invocation
linux - user / signal . c : Don ' t pass sigaction uninitialised sa_flags
target - arm : check that LSB <= MSB in BFI instruction
virtio : add check for descriptor ' s mapped address
target - arm : Fix TCG temporary leaks for scalar VMULL
qxl : clear guest_cursor on QXL_CURSOR_HIDE
check for bs -> drv in bdrv_flush ( Christoph Hellwig )
ide : ahci : unparent children buses before freeing their memory
target - i386 : Don ' t left shift negative constant
hw / i386 / acpi - build . c : Fix memory leak in acpi_build_tables_cleanup ()
tests / bios - tables - test : check the value returned by fopen ()
fsdev : Fix overrun after readlink () fills buffer completely
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
qga - win32 : Fix memory leak of device information set
i386 : hvf : inject General Protection Fault when vmexit through vmcall
target / ppc : Fix wrong number of UAMR register
io : assert errors before asserting content in I / O test
qcow2 : Fix alloc_clusters_noref () overflow detection
spapr : drop reference on child object during core realization
spapr : ensure we have at least one XICS server
block / iscsi : fix memory corruption on iscsi resize
APIC : Do not start zero - period timers ( Jan Kiszka )
linux - user : don ' t crash with null name
block : Don ' t use error_abort in blk_new_open
hpet : Fix emulation of HPET_TN_SETVAL ( Jan Kiszka )
chardev : clear O_NONBLOCK on SCM_RIGHTS file descriptors
slirp : don ' t crash when tcp_sockclosed () is called with a NULL tp
tcg - sparc : Fix small 32 - bit movi
target - microblaze : dec_barrel : Plug TCG temp leak
virtio - net : properly check the vhost status during status set
vhost - user - test : fix predictable filename on tmpfs
qemu - img convert : Use cache = unsafe for output image
pci : move dereferencing of root only after verifying valid root pointer
fsdev / virtfs - proxy - helper : Fix improper use of negative value
fsdev : Fix potential memory leak
vmdk : fix double free
stdio_pclose : return - errno on error ( v3 )
target - tricore : Fix helper_msub64_q_ssov not reseting OVF bit
hw / intc / arm_gic : Fix NVIC assertion failure
vpc : fix return value check for blk_pwrite
arm : fix compile on bigendian host
qcow2 : Use sizeof (** refcount_table )
target - ppc : Fix use of uninitialized TCG variable in tlbiva
check malloc return for code in rabin2 . c
prevent a possible double - free
fix memory leak
Fix a memleak
add warning to zos command (# 8585 )
versioninfo : Fix SEGV on files with no info (. txt , ...)
Corrected a condition on the first op
take bits " wildcard " into account in r_bp_size (# 8994 )
Check if debug is dead in r_debug_kill function .
Fixing the breakpoint callback signature . (# 10998 )
CID 1134732 Out - of - bounds access / libr / asm / arch / avr / format . c
fix hang in version info ELF
Oops for ' dk '
format / zimg : make sure there are enough bytes in the buffer
bin : Delete the list of objs instead of deleting current object
riscv : Return actual instruction length
Disable self :// in sandbox mode
bail out when no proxy was configured
pid - 1 is not our child
Initialize all bits in the sigaction .
varnishncsa : Don ' t output ESI subrequests
Remove a now pointless assert .
Delete the right list item .
note a TODO
Bail out of CLI reads on signals
extra check in mg_start (): options may be NULL
Added connection - specific conn_data pointer to struct mg_request_info
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
plugins / python : fix tracebaker resource leaks on import error
fixed sigquit / sigint in hijacked mode
fixed goodbye_cruel_world in gevent mode
Added an additional check in some code to avoid dividing by zero ...
Attempt to appease travis
Units / Map : added an empty () function
idle controlled teams serialize as " human " controlled
updated about : Guillaume Duwelz - Rebert removed from developers
fixed bug where entering a chat message while the message is selected ...
Set the core_id preferences item at game launch .
Added dedicated fallback message if a replay is loaded from mp create
Fixed a missing brake in a switch case construct , spotted by Crab .
Update the GUI when using [ replace_schedule ]
Initialize all members .
use a list for the standard message receiving sound
dont allow empty indexes in wml variables
Initialize all members .
fix default room creation policy on server
add game start notification
made it so AI players have an empty description in multiplayer games
Initialize all members .
Fix : with -- new - widgets advanced preferences may not shrink on deselect
locations not on the map should instantly fail to location filter
AI : ensure multiple weapons are always checked for [ disable ] special
added Jonatan Alam � as catalan translator
Initialize all members .
fix ReadOK macro to avoid casting warnings
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
systemctl : fix memory leak (# 7289 )
conf - parser : fix strv parsing
fix inotify to work not only once
sysusers : fix memleak (# 4430 )
ude_rules . c : fix possible NULL pointer dereference in get_key ()
rfkill : Avoid error when state restore is disabled
bus : fix issue with reference counting
core : kill processes started due to the ExecReload = on timeout
hibernate - resume : refuse to run outside of an initrd
load - fragment : setting empty string to Nice = resets the previous assignments
tmpfiles : add missing OOM check
terminal / screen : perform bold -> light conversion only on foreground
basic / strv : use existing qsort_safe () helper
sd - event : fix theoretical leak on error in event_make_signal_data
logind : fix printf format
systemctl : do not print header if no units will be listed
fuzz : fix coding style
systemctl : Add missing "/" to files created by ' edit -- runtime '
basic : use for () loop instead of while ()
systemctl : never check inhibitors if - H or - M are used (# 3781 )
sd - dhcp - client : fix busy loop reading zero sized DHCP UDP packets . (# 3299 )
path - util : plug leak
bus - util : fix error handling
sd - bus : fix object tree to be deeper than 2 levels
mount : fix for complex automounts
fstab - generator : fix trivial leak
util : make sure we don ' t clobber errno in error path
udev : settle should return immediately when timeout is 0
cgroup : the root cgroup is always populated
bus : in sd_bus_try_close () consider local read queue too
terminal / subterm : use usec_t instead of " unsigned long "
udev : properly detect reference to unexisting part of PROGRAM ' s result
machinectl : make sure we are not reading an unitialized variable
switch - root : not only mkdir the parents but the target dir
pam : skip DBUS_SESSION_BUS_ADDRESS export when kdbus is not active
util : be a bit safer in path_is_safe ()
resolved : use dns_answer_size () where appropriate to handle NULL DnsAnswer
journald : fixed assertion failure when system journal rotation fails (# 9893 )
random - util : we are fine if ints are 16 bytes actually
core / manager : Make sure jobs statistics are not double after daemon - reload
resolved : bugfix of null pointer p -> question dereferencing (# 6020 )
calendarspec : free memory when parsing fails (# 4890 )
don ' t choke if there are no sockets for a service
sd - dhcp - client : readd deleted error check
sd - resolve : fix check for packet size
exit - on - idle : only exit if actually idle
usb_id : MassStorage SubClass 6 is " scsi " not " disk "
readd early log level check for performance reasons .
Patch to log security warnings when invalid identifier is used in message
Call all configured exec_dump_fn ' s when SIGUSR2 is received
set serialize lock to proper function .
Stop the totem stats updater timer during shutdown to prevent references to
totemcrypto : fix hmac key initialization
totemsrp : Enhance mcast failure detection
libdocument : return 96 DPI if unable to determine true value
Prevent jumps to last page when autoscrolling
libview : Add check for pageless documents to ev_view_accessible_focus_changed
[ libview ] Do not jump to link dest when clicking with GDK_CONTROL_MASK
libview : Do not schedule a redraw when selecting all text
Bug 493778 – metadata plug - in crashes on some images
app : don ' t leak the GtkTextBuffers of Gegl multiline text properties
app : make LCH lightness select - criterion faster
app : clear current path from the context after copying it
plug memory leak : Fixes # 305990
app : prevent warnings in gimp_levels_tool_config_notify ()
Bug 791514 - Cannot export to webp file .
app : update ` gimp - xcf - save ` procedure documentation .
Make the preview column the expander column
app : silence a harmless critical warning
libgimp : disable tile command range check in plug - in tile backend
Bug 595605 - memory leaks in plug - ins / common / curve - bend . c
plug - ins : Use string literal as format in metadata plug - in
app : initialize variable in gimp_histogram_view_update_bins ()
app : finish porting GdkScreen / monitor_number to GdkMonitor
app : set an icon on the floating selection filter
Bug 686379 : In the JPEG save dialog , checking / unchecking XMP ...
app : typo : make string translatable
app : fix rectangle select tool cursor after committing / halting
app : add a tooltip to the color temperature preset menu button
Bug 645120 - Disable color tools overlay dialogs
app : avoid id . matrix short - circuit in trans . tool when ...
[ ot ] Ensure layout in hb_ot_layout_table_get_lookup_count ()
[ ot - font ] Fix hmtx wrong table length check
Bug 528272 – " Error loading GPOS table 5503 " when using katakana
Remove a " register " keyword from C ++ code
branches / zip : btr_search_guess_on_hash (): Do not call buf_block_get_frame (),
Remove the temporary file created by mysql_upgrade
branches / zip : lock_update_discard (): Fix a bug introduced in r949 :
Whoops , freed something not malloced
fix breakage on qnx
Fixed issue with retrying autocommitted transactions . We might need to
branches / zip : Restore the transaction commit that was removed in r1583 .
branches / zip : btr_cur_optimistic_insert (): Remove a redundant condition .
initialize mysql -> charset in mysql_init
fixed bug # 1661 Compiling -- pstack fails with binutils 2 . 13 . 90
sync0arr . c : Fix compilation error on some systems by introducing all
branches / zip : row_sel_sec_rec_is_for_clust_rec (): Augment the bug fix in r918 :
branches / zip : Deny non - superuser access to the INFORMATION_SCHEMA
branches / zip : ha_innobase :: add_index (): Check for ! innodb_table .
sync0arr . c : Fix compilation error on some systems by introducing all
followup to SET PASSWORD fix
Added missing prototypes to mysys / my_bitmap . h
refs # 5560 , fix bug
allocate an array of pointers to ctpair ' s rather than an
Addresses # 1683 Assert shutdown is not called twice on the same minicron
refs # 5133 , fix compilation error
Fix a warning about extra parenthesis
branches / zip : buf_LRU_block_remove_hashed_page (): Add some debug assertions .
trx_undo_parse_page_header (): Silence a bogus warning .
[ t : 4472 ], add table lock for truncate
connect zip bug fix
fix a build problem . addresses # 1396
[ t : 5131 ] closes # 5131 .
Applying InnoDB snapshot
Fixed a bug in vis . c
Fixed bug in sub select after last merge
fixed picture in comment .
fixed BINLOG crashes in case of badly formed statement ( Bug # 17457 )
correct fix for the bug # 6151
fix for decimal . c
bugfix : delayed insert table was using other table ' s expr_arena
branches / innodb +: row_purge_remove_sec_if_poss_low (): Remove a suspicious
[ trunk ] Import commit 8ec8321df613986e8642e2975f7182072eb4be62 from ghostpdl
src / bin / jpwl / convert . c : fix memleak ( fix suggested by maddin200 , # 631 )
[ trunk ] Fix simple warning about sign conversion
[ trunk ] Fix Heap - based buffer - overflow when decoding openjpeg image
r15723 @ tombo : nickm | 2007 - 12 - 26 19 : 25 : 51 - 0500
fix duplicate dns_cancel_pending_resolve reported by Giorgos Pallas
Add a missing return - 1
r8682 @ Kushana : nickm | 2006 - 08 - 29 17 : 58 : 59 - 0400
fix crash introduced in r9622
Fix a check - spaces complaint
r11753 @ Kushana : nickm | 2006 - 12 - 29 11 : 30 : 28 - 0500
fix memory leak introduced in 26e89742
fix a memory leak
take back half of my bugfix . comparing times sucks .
clean up a log message
Bridges are always dirs
fix a seg fault with exitredirect
Treat a bad tor_spawn_background () as a BUG ().
Fix a potentially useless integer overflow check .
initialize memory to remove Valgrind warning ( Zooko , Jeffrey Walton )
Reverted change in config . h as it does not fix anything .
fix jsvGetString regression
fix precedence of ' void 0 '
fix wifi restore ssidAP
layouts : fix memory leak
replace signed with unsigned counter to avoid warning when compiling with
Initialize the entire launch structure .
Added missing unlock for pre - mature return from log_msg . - Jette
change the pthread locking to eliminate a race condition
fix for checking valid qos
removed debug message
fixed last patch
Initialize variable to eliminate clang error
fix for qos char list
fixed minor memory leak
Prevent zero divide
Missing batch job given exit code of 1
Always pass non - NULL status reference to job_waitjid , that function in
set version to 0 . 5 - dev
fix uninitialized text outline width in error image
Fixed bug 769 , duplicate error with TILEITEM .
Added the declaration for freeStyle to eliminate the unresolved externals .
fix leak on group_layers
Changed default styleObj offsets to 0 , duh ...
Set the WSQ TLS key to inited after initing it .
[ runtime ] Speed - up MERP timeout
do not exit after the first read !
[ sgen / mono ] Don ' t leak stack info struct
Fix the previous change .
[ llvm ] Fix 32 bit platform support which got broken after c678533842f10356968c203ed8fa43e86a406c6e .
tasklets . c : update used stack size
[ profiler ] Use thread_exited profiler event for recording thread end events .
Fix a bug introduced by the previous change to mono_jit_walk_stack_from_ctx_in_thread ().
[ runtime ] Fix minor leak
[ coop handles ] Fix monotonic handle check race with stw in mono_handle_new
[ runtime ] Free leaked variable . (# 4643 )
[ sgen ] Save more space in M & S block infos .
[ runtime ] Don ' t free null cache hash table in aot compiler
[ llvmonly ] Avoid disabling llvm for methods with pinvoke signatures in llvmonly mode .
[ profiler ] Replace a check with an assert in coverage_filter ().
[ runtime ] Free LLVM failure message
[ runtime ] Fix malloc backend for mempools .
Fix the build .
[ runtime ] Make mono_trace_set_printerr_handler () call g_set_printerr_handler (). Fixes # 31451 .
* mini - s390 [ x ]. c ( is_regsize_var ): Support PTR / FNPTR too .
[ runtime ] Free temporary attr structure
[ jit ] Avoid emitting seq points in inlined methods .
[ jit ] Fix the handling of byref locals in mono_allocate_stack_slots2 ().
Fix a JIT crash when - v - v - v - v is used .
[ aot ] Fix a memory leak .
geoipbackend : Avoid expensive destroy during initialization
dnsdist : Fix a sign - comparison warning in isEDNSOptionInOpt ()
dnsdist : Flush output in single command client mode
Check the packet size in dnspcap2protobuf
auth : Reset the TSIG state between queries
Fixed missing initialization
Prevents a possible memory leak
Added missing free ()
Fixed possible NULL pointer exception in ssl_get_ciphersuite ()
pathbar : don ' t unconditionally iterate over buttons when scrolling
window - slot : protect against no new view
desktop - background : stop any pending crossfades on widget destruction
toolbar : plug a couple of memory leaks
have to call delete_files before done ; oops
pathbar : set right return value for class method function pointer
Ignore > 2 clicks in the icon container (# 621098 ).
mime - actions : fix double free in error path
files - view : select the pending selection
search - hit : fix mem leak
window : open detached tab windows at mouse position
Added # prog_id import syntax for TSs without SDT
fix memory leak in MPD Byte range objects
fix buffer overrun in gf_bin128_parse
[ bouqueau ] fix win32 / mobile build
fixed # 1769 ( fuzz )
Commented safety check by romain breaking SVC import
Fixed bug in image loader
Removed scanning files starting with '.'
Fixed wrong AU seq num len when hinting (# 640 )
don ' t try to dump null boxes (# 1093 )
fixed potential crash in adts reframer with broken streams - cf # 1723
fixed UNKNOWN READ
jcop : coverity scan warning RETURN_LOCAL
fix trivial leak
OpenPGP : NULL - ify free ()' d pointer
sm - common : fixed resource leak
reader - pcsc . c : fixed pin checking when implicitly given
libopensc : initialize value returned by sc_select_file
remove pointer to freed secret object
Fixed out of bounds access in ASN . 1 Octet string
pkcs15 : log parsed TokenInfo . serial
fixed parsing pace output data
prevent out of bounds write
psql : honor ' footer ' option for expanded output
Check that result tupdesc has exactly 1 column in return_next scalar case .
Add an explicit cast to Size to hyperloglog . c
Fix a bug in handleAssertionEffects
exif_scan_JPEG_header : Added length check to prevent information leak
adding Content - Length for HEAD request
Fix a fallthrough bug in TranslatorX64 :: smash
Fix a copy - paste error
tdf # 96243 don ' t crash if LibO install . wasn ' t found
[ XrdCl ] Prevent segv in case user has no name .
XrdHttp : Add missing return statement
[ XrdCl ] Initialize pStateRedirec to avoid a segfault while recovering
[ SSI ] Unbind the request prior to teardown .
groupserv : Actually free mygroups and groupacs .`
libathemecore / help : Fix memory leak when a help file cannot be opened
chanserv / moderate : don ' t moderate registrations from those who have PRIV_CHAN_ADMIN .
fixed sendfile core dump .
added more extension info .
bzero when close new_fd .
Missing delete for config reader
Copy the silence flags when sending update notifications .
Fixed some craq ++
Ignore this commit . * jedi mind trick *
nickflood : Allow changes to UID
Fix a bug with / SHUN deletion ( it didn ' t work )
Fixed / spynames , thanks to relax for finding the bug
Made erroneous included configs cause the reader to bail
Convert the output limit back to the parameter
m_spanningtree Ensure that TreeSocket :: Close () only runs once
win : Fix setting icon after format of atom . rc has changed .
added comment to mention sk_tools_utils :: copy_to
Closing asar file should be syncronous
unzip : comment why there is no USE_INTERNAL_MINIZIP boolean option
* Fix to the gcc4 /- O0 x86 JIT compiler bug
Invalid values for saved_stamp in comput_subject_downtime_times ()
ospf6d : don ' t run DR election early on " ipv6 ospf6 priority "
pimd : Fix size_t zlog_err format string warning
[ bgpd ] 64 - bit bugfix in community_del_val by Jeremy Jackson < jerj @ coplanar . net >
ospfd : fix packet reception for FreeBSD 10 .
* src / transform . c ( transform_save ): append newline to old text
* src / syntax . c ( print_tree ): print something for a NULL tree
* examples / fadot . c : free the result from FA_AS_REGEXP
avoid symlink attacks in __fish_print_packages and spawning fishd
env_universal_common : always pick shmem strategy on Cygwin
fish_indent : indent " case " with the same level as " swith "
prevent out - of - bounds ensure clause access ; fix # 2910
peephole optimization for LOADNIL before STRCAT ; ref # 3110
undef append_features for Classes
check ` mrb_open ` return value
Fix an off - by - one causing a buffer overflow in mirb .
forgot to free irep -> reps
fix length check in read_section_debug (); fix # 1572
fix String # inspect with MRB_UTF8_STRING
eliminate an LLVM warning about an unused value
silence a coverity warning
x509 : free shallow general names after requesting crl fetch
proper fix for mtu calculation initialization
Get openswan to send the remote host address to PAM during XAUTH
Prevent the LDAP validator from accepting an empty password .
Fixes a tiny memory leak on the Digest ' nonce ' cache code .
Fixes an issue processing the X - forwarded - for header . Bug # 924 :
cherokee_buffer_crc32 () did not handle empty buffers correctly .
Fixes a memory management issue . The ' extensions ' rule match could
xkbcomp : fix stack overflow when evaluating boolean negation
compose : fix infinite loop in parser on some inputs
x11 : add 0 < ctrls -> numGroups <= 4 assertion
src / utils : check if fileno () failed in map_file
xkbcomp : Don ' t explode on invalid virtual modifiers
xkbcomp : fix pointer value for FreeStmt
Remove a gcc - 4 . 5 compilation warning :
fix PTRDIFF_MAX check in the 32 - bit mmap wrapper
bionic : fix memory leak in get_malloc_leak_info () error path
Libraries without dt_soname are inaccessible
Free the WSTR version of the filename when dead .
swb : fix one missing inlineSlots annotation
Use 16pix icons for size < 19
squash heap - buffer - overflow , PoC TIFF from Hardik
Added check for incorrect number of meta channels report in # 492 .
Added missing null check reported in # 817 .
Fixed SEGV reported in https :// github . com / ImageMagick / ImageMagick / issues / 130
Added missing return to avoid use after free .
Forgot to factor in softness
Restored call to RelinquishMagickResource .
fix memory leak in ReadYCBCRImage as SetImageExtent failure
Fixed incorrect call to WriteBlob reported in # 490 .
Fixed incorrect call to DestroyImage reported in # 491 .
Fixed memory leak .
Added check for bit depth 1 .
fixed a server crash
fixed input problem , partly :)
fix a leak when unpausing evrpc requests
reset " chunked " flag when sending non - chunked reply
avoid valgrind false positive by zeroing epoll_event
initalize ev_res from Scott Lamb
* ftpclass . cc : don ' t send FEAT after QUIT .
mmv : use xstrdup instead of strdup ( for allocation tracking ).
correct a signedness warning
win : add missing break statement
Windows : set loop counters to zero in uv_loop_init
unix : don ' t run i / o callbacks after prepare callbacks
unix : NULL pipe_fname in uv__pipe_close ()
unix : fix off - by - one error in comparison
uv - unix : fix warning : control reaches end of non - void function
unix : handle EWOULDBLOCK in uv__write ()
X3F / simple_decode_row : check for data offset limit
Plugin : add export to buffer_size function .
palm : getpeername may fail , found by coverity
mk_core : thread : fix ucontext . h path
utils : Fix a valgrind warning about uninitialized data
server : http_parser : fix pointer protocol length
Sched : Fix a segfault when iterating timeouts
mk_server : streams : fix dereference after null check ( CID 1312109 )
bin : declare missing server ctx
window - tracker : Fix memory leak
shell - doc - system : fix %- escaping code in shell_doc_system_open ()
recorder : Don ' t compute the buffer size twice
st - box - layout : Don ' t report a paint volume when we have no allocation
browser - plugin : Set that we need XEmbed
[ ShellApp ] Signal windows - changed when removing a window
detect : add missing sigorder case
stream : cast memcap checks to uint64_t
fix seg fault due to wrong sm list access in hscd
dnp3 : use BasicSearch instead of memmem
defrag : fix reconstruction
counters : don ' t run if no counters have been registered
http_heade fix , check first that we have a connp before checking transactions
flowvar : initialize new memory to prevent issues on error handling
detect : free rule group init memory at the end of startup
fix uninit var usage in hhd
reject pcre modifiers U with B
dns ( tcp ) - fix coverity cid 1374307
pe : imports : Actually look at FirstThunk if OriginalFirstThunk is zero .
http2 : support > 64bit sized uploads
get_url_file_name : Fixed crash on OOM on debug build
pop3_doing : don ' t call pop3_dophase_done () if already failed
multi : fix CURLM_STATE_TOOFAST for multi_socket
imap . c : Fixed an incorrect variable reference
mime : properly unbind mime structure in curl_mime_free ().
tool_writeout : fixed a buffer read overrun on -- write - out
gtls : dereferencing NULL pointer
http2 : Fix address sanitizer memcpy warning
Allow compiling under Android
fix compiler warning
Fixed debug . c to work again unchanged .
voutf : fix bad arethmetic when outputting warnings to stderr
urlglob : avoid NULL pointer dereference
curl : do bounds check using a double comparison
[ SP ] Fixed incorrect cgame syscall argument handling for Menu_Paint .
[ SP ] Win32 : Fix game incorrectly reporting low mem
[ MP ] Fix an incorrect memcpy count argument in CQuickSpriteSystem :: Add .
[ MP ] Fix crash when drawing input fields
[ MP ] Prevent a crash in NPC_ChoosePainAnimation when NPCs are knocked down .
Fixed GL error with SDL on startup if not using nvidia card .
fix typo : s / POWOR / POWER /
get rid of strcat
ext / syck / handler . c ( syck_hdlr_add_alias ): add cast to avoid warning .
ext / zlib / zlib . c : initialize return value
comma at the end of enum is a C99ism
string . c : fix one - off bug
string . c : fix integer overflow
thread_sync . c ( condvar_ptr ): reset fork_gen after forking
st . c : fix num_entries
Allow clients to have a resolved hostname of localhost
modules : wow , that was a dumb error .
m_stats : don ' t try to access bl_stats if it doesn ' t exist
blacklist : this didn ' t get committed somehow
opm : properly disable opm on blacklist deletions .
Windows : terminate more aggressively when you close UnrealIRCd .
[ git / 2 . 1 regression ] Fix opj_write_tile () failure when numresolutions = 1
Cast to size_t before multiplication
Fixed possible NULL pointer exception in ssl_get_ciphersuite ()
fix error from commit 13585f15c7f7dc28bbbba1661efb280d530d114c
Shut up gcc
diraliases : always set the tail of the list to NULL
Spit out definition of YY_CHAR early
cmd / 8g : don ' t crash if Prog -> u . branch is nil
cmd / gc : rephrase the invalid indexing operation error message
initialise dc variable to satisfy old compilers .
Unauthenticated DH client certificate fix .
typo fix on function
Fix a memory leak in tls1_mac
X509_VERIFY_PARAM_free : Check param for NULL
Typo : should check mgf1md
x509 : fix double locking problem
RT4002 : check for NULL cipher in p12_crpt . c
Fix a possible crash in dsa_builtin_paramgen2 .
set auto ecdh parameter selction for Suite B
make sure we don ' t write to seed [- 1 ]
Avoid a memory leak in OCSP_parse_url ().
Free a BIO_ADDR if DTLSv1_listen return <= 0
Missing OPENSSL_free on error path .
fix previous modification -- if ssl -> cert is NULL , don ' t follow the pointer .
Updates from 1 . 0 . 0 - stable
usbutils . c uninitialised usbstat for non - primary mmqs
klondike use a memcpy
policycoreutils : semodule_package : do not fail with an empty fc file
libsepol : sepol_av_to_string : clear static buffer
libsepol / cil : Destroy classperm list when resetting map perms
libsepol / cil : Destroy classperms list when resetting classpermission
libsepol / cil : cil_reset_classperms_set () should not reset classpermission
fix small memleak in action ()
bugfix : check return value of malloc ()
bugfix : plug a memleak in mmsnmptrapd
fix small memory leak with string - type templates
core bugfix : small memory leak in external command parser
fixed memory leak in worker instance of omelasticsearch
core : replace constant by sizeof in a call to snprintf
irc : fix parsing of DCC filename
relay : remove unneeded check on variable irc_argv
Avoid a double - free when a window size of 0 is specified
Issue 719 : Fix for TALOS - CAN - 154
warc : consume data once read
cast DWORD to int to match % d printf specifier
Fix a crash with xmllint -- path on empty results
Check the result of EC_GROUP_new_curve_ * calls
crypto : Enable EDDH all OpenSSL cryptolib over beta version 1 . 1 . 1 - pre8
erts : Fix beam_load assert
fix : don ' t pass colour - codes to filters
range check - set_default_value
Handle NO response without message properly
proposed fix for clearing labels
Try fixing a possible segmentation fault . I ' m not entirely sure how
imap_quote_string make room for quotes
Try to fix # 302 .
klondike - fix isc uninit warning
fpgautils : add support for 57 . 6 kBd serial
Target should only be 32 bytes copied .
Fixed one byte stack overflow in mcast recvfrom .
Bugfix : Free filename entry for writing config file when done with it
Bugfix : Missing pool_no parameter to applog for no - stratum - sessionid debug message
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
abrt - gui : change the help accelerator to alt - H
remove some logs
Delete the TransferManager on disconnect
emacs bindings : added M - d ( kill word )
fixed multiline history
add svn_version () and global var which are
src / wav . c : Fix exif parser bug
src / pcm . c : Improve error reporting .
src / wavlike . c : Prevent infinite loop on malformed file
src / aiff . c : Validate channel count .
[ trunk ] added box length inconsistency check
[ trunk ] Import patch from sumatrapdf team . This handle testcase 2977 . pdf . asan . 67 . 2198
Fixing exit - after - terminate when used with bare mode .
reset ` reprocess_is_too_early ` when reprocessing
rhbz : die if missing mandatory member
strbuf :: strtrimch () : memmove () reads bytes out of borders
cli : remove superfluous problem_data_free () call
bplist : Make sure to detect integer overflow when handling unicode node size
xplist : Prevent heap buffer overflow when parsing empty tags
fix crash on unknown shape - reported by Ryan Schmidt
quickfix for metapost
Fixed glib warning with autologging and queries .
Fixed potential misinterpretation of boolean logic
define IN6_ARE_ADDR_EQUAL macro for WIN32
Fixing ecma_create_arguments_object : adding deref for allocated ecma - string
Adding assertion that interpreter doesn ' t receive non - empty normal ecma completion value .
IEEE 802 . 11 : Add a bounds check
Fetch a 32 - bit big - endian quantity with EXTRACT_32BITS ().
PPP : check PAP AREQ length too , see commit 32118c6
needed for previous checkin
Remove too early set of auth_user
Check if auth_user is set .
Check if auth_user is set .
fix a warning i just introduced
added missing g_free ()
add missing g_free
Free the decision
cbson : PySequence_GetItem returns a new ref
send . c : if send_ra fails , return - 1
set_interface_var () doesn ' t check interface name and blindly does
[ Bugfix ] Render " Ok " and " Cancel " button correctly when compiled with GNUTLS .
fix leak in handle_propfind ()
gcc complains about _MSC_VER being an undefined macro .
Fix an allocation problem in inf_text_fixline_buffer_buffer_insert_text ()
CID 90884 storage_db : Check for bad data type
initialize unused pointer to prevent freeing without allocating
idevicebackup2 : Only post " sync finished " notification if we sent " sync start " earlier
idevice : free ssl object when SSL handshake fails
fixes depth check in format_string ( can be 0 )
mobilesync : Plugging a memory leak
Fixed an array overflow problem in the JPC decoder .
don ' t echo query params on invalid requests to redirect URI ; closes # 212
Avoid a memory leak on non - PAM systems
Add some messages to PAM close session
API : Deprecate __index__ for ndim > 0
BUG : Remove memory leak in np . place
BUG : erroneous use of sprintf
tcti_ldr : load a specific so version
fix : prefix 0x67 missed .
[ M68K ] handle -> printer_info need to be check
systemz : fix # 1062
mips : return Fail on assert failure in MipsDisassembler . c
Fixing ' in ' opcode handler .
cgfs : fix dorealloc ' s batch allocation
pass the UDI up if existant
fix accidently reverted code
osdep : Fix possible infinite loop ( Closes : # 1582 ).
dracut - install : handle more ldd errors
libcephfs : destroy cmount on ceph_shutdown ()
mon : fix bad return test !
msg : MOSDSubOp has a private destructor
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
filejournal : use :: encode () wrapper func for Transaction
rgw / rgw_rest . cc : fix 4K memory leak
fixed non - root rooted path issue
filejournal : initialize header before writing
lockep : fix leak in failure / assert path
mds : shut up about request latency
librbd / internal . cc : close resource leak
PG : clear peering flag when we transition from Peering to Reset
crypto : don ' t hash zero sized buffer
osd : Reset () the snaptrimmer on shutdown
mon : send mgrdigest promptly to new active mgr
rgw : Error check on return of read_line ()
cls / rbd : fix note
set bl length in buffer :: read_file
librbd : force - remove journal when disabling feature and removing image
rgw : fix printf format warning
librados : fix unsigned vs . signed warning
osd : avoid polluting peer_info with acting [ 0 ]
mds : handle MMonMap
rgw : add xml output header in RGWCopyObj_ObjStore_S3 response msg
Event : process event before pop it to avoid releasing
rgw / rgw_rest . cc : fix uninitialized static struct
PG : strays don ' t peer ( only the primary does )
doc : Add usage comment to librbd :: RBD :: AioCompletion
mds : put pending ( not added ) caps in reply caps
msg : initialize footer
rgw : AbortMultipart request returns NoSuchUpload if the meta obj doesn ' t
osd : Update object state after removing watch from object info
rgw : fix flush_read_list () error msg
msg : MLogAck has a private destructor
OSD : lock recovery_wq before debug output on finish_recovery_op
rbd - mirror : gracefully fail if object map is unavailable
osd : fix rare memory leak
rgw : move coroutines out of anonymous namespace
OutputDataSocket : fix uninit var
mds : look at active ( not projected ) inode when issuing truncation
msg : MHeartbeat has a private destructor
kclient : fix snap_rwsem link on non - existent cap release
mon : initialize vars
correct error of repeatedly adding io_size to off in do_bench_write
osd : init my_stat
osd : drop obsolete ' filestore ' bool option
mds : set bad dirfrag ' s version to 1
msg : MMDSMap has a private destructor
rbd : allow users to remove rbd image when Ceph cluster becomes full .
mds : fix can_rdlock_soon vs xlock on FileLock
mgr : fix OSDMap notifications
filestore : free fiemap used for testing fs capabilities
rgw : bi_list () initialize is_truncated if - ENOENT
crush / CrushWrapper : fix out of bounds access
filestore : sync object_map on _set_replay_guard ()
msgr : fix problem with forced stop of pipe
add rename check_access
osd : fix trailing + in pg state string rendering
osdc : fix pgls_filter op encoding
mds : add IFLOCK to wait bits to prevent collisions with lock branch
kclient : reset session cap seq # on reconnect
osd : fix build_initial_pg_history
OSDMap : reset osd_primary_affinity shared_ptr when deepish_copy_from
osd : fix leaked OSDMap
mon : Monitor : drop useless ' todo ' comment
ReplicatedPG : CEPH_OSD_OP_WATCH return - ENOENT if ! obs . exists
librados : fix use without NULL check in rados_pool_list
msg : MClientLease has a private destructor
config option for mon ' s DNS SRV records
bug - fix for bz # 1352432
Fixed malloc . h path on iOS target platform
jpeg : Fix another possible buffer overrun
Added vim modeline
Exit the max size loop when there ' s nothing left possible to find
fix memory leak
tar : fix crash on broken tar file .
Add a bounds check to ByteArray :: Get ().
Issue 23433 in oss - fuzz : Heap - buffer - overflow in findNextBorderPixel ()
added missing pclose to stop zombies from forming
fix memory leak .
gitlab # 179 - rsvg_filter_primitive_free (): Check the GStrings for NULL before freeing them
bgo # 748608 - Destroy cairo_t after use
dosfslabel : Do not read beyond string length ( Closes : # 709587 ).
Be more careful when calculating sinc () for small arguments
Fixed a memory leak in the BMP decoder
apps / sipdial : add support for sips : URI scheme
ares : Prevent buffer overlow in ares_parse_a_reply ( CVE - 2017 - 9454 )
buildFHSUserEnv : change to root directory after chroot
util : make sure to close the authfile before returning success
Fixed memory leak when an invalid timezone was found
Flush the bus before exit
remove invalid free
regcomp : use long jumps if there is any possibility of overflow
regcomp . c : Prevent integer overflow from nested regex quantifiers .
fixed memory leak
libvncserver : scale : cast to 64 bit before shifting
LibVNCClient : remove now - useless cast
rfbserver : don ' t leak stack memory to the remote
libvncclient : free vncRec memory in rfbClientCleanup ()
libvncclient : bail out if unix socket name would overflow
Bug 700947 : Add missing ENDTRY opcode in try / catch / finally byte code .
qemu : Add missing lock in qemuProcessHandleMonitorEOF
Always check the number of coefficients
Fixed possible credentials leaking reported by Alex Birsan .
hfs : fix keylen check in hfs_cat_traverse ()
Changed to using carbon module delegate methods
Log an error when SPIFFS_mount fails
typo in shader
stb_leakcheck : Make stb_leakcheck_free actually free .
Applying InnoDB snapshot
continued fix for reset master
branches / zip : Check for autoinc next value overflow explicitly when offset
fix Bug # 15907954 - SETTING INNODB_MIRRORED_LOG_GROUPS = 2 LEADS TO SERVER
Added missing DBUG_ENTER ( bug in last push )
Added missing DBUG_RETURN
fixed compiler error
stole magnus ' fix for random im test failures from 5 . 1 - maint
Addendum to the fix for bug 43029
typo in libedit fixed - possible buffer overflow - bug # 4696
fixed bug # 1661 Compiling -- pstack fails with binutils 2 . 13 . 90
pb2 fix : one machine on pb2 didn ' t like TABLE :: map
Tracing test failure : testRedo - n CheckLCPStartsAfterNR T1
blockSend and unblockSend are not declared since
correct parameter of hash_search
Fixed a problem with Innodb_buffer_pool_pages_latched status variable .
branches / zip : ha_innobase :: add_index (): Check for ! innodb_table .
cleaned up lots - o - warnings build
fix aes ctr cast
Fixed issue with ECDH - ECDSA cipher suites rejecting certificates without
discard session chain in SSL_clear
set correct cert type in CertificateRequest when using ChaCha suite with ECDSA
Fix the WOLFSSL_SEP (-- enable - sep ) build scenario where extCertPoliciesNb is not available .
lib / string . c : add ULL suffix to the constant definition
[ PATCH ] uml : activate_fd : return ENOMEM only when appropriate
Btrfs : add tree block level sanity check
drm / mgag200 : Constify function pointer structs
sh : Use __GFP_ZERO for dma_generic_alloc_coherent ().
[ PATCH ] tpm : fix memory leak
drm / fb - helper : Do the ' max_conn_count ' zero check
staging : comedi : adv_pci_dio : do board reset early in (* auto_attach )
i2c : xiic : must always write 16 - bit words to TX_FIFO
dlm : sctp_accept_from_sock () can be static
module : printk message when module signature fail taints kernel .
intel - iommu : double kfree ()
drm / i915 : Make disable_cursor_plane similar to commit_cursor_plane .
usb : gadget : f_subset : fix missing unlock on error in geth_alloc ()
signal : always clear sa_restorer on execve
fs / binfmt_misc . c : add terminating newline to / proc / sys / fs / binfmt_misc / status
sched / deadline : Skip in switched_to_dl () if task is current
[ SCSI ] fcoe : fix missing error check in call to fcoe_if_init
[ SCSI ] mpt fusion : make mptsas_smp_handler update resid
wlcore : fix unsafe dereference of the wlvif
mm : highmem : export kmap_to_page for modules
[ IPSEC ]: Fix inetpeer leak in ipv4 xfrm dst entries .
efikasb / mx : fix usbh1 initialisation
staging : rtl8192e : don ' t just print an error and continue
e1000 : only enable TSO6 via ethtool when using correct hardware
rmap : recompute pgoff for unmapping huge page
userns : don ' t leak root user
arm64 : Align CMA sizes to PAGE_SIZE
[ IPV4 ]: ipv4_is_lbcast () misannotations
KVM : MMU : fix shrinking page from the empty mmu
quota : info leak in quota_getquota ()
[ PATCH ] it87 : Fix oops on removal
ASoC : OMAP : HDMI : Initialize IEC - 60958 channel status word
cgroup : make sure memcg margin is 0 when over limit
drm : work around EDIDs with bad htotal / vtotal values
KVM : PPC : e500 : Eliminate preempt_disable in local_sid_destroy_all
loop : Update mtime when writing using aops
mtd : orion_nand : Fix build failure caused by typo
iwlwifi : fix kernel oops when ucode DMA memory allocation failure
bridge : cleanup : remove unneed check
crypto : atmel - sha - add new version
[ S390 ] cio : fix orb initialization in cio_start_key
af_rxrpc : Handle frames delivered from another VM
iwlagn : check beacon frame size
isicom : Deinline drop_dtr , save 112 bytes
drm / vmwgfx : Fix kernel NULL pointer dereference on older hardware
iio staging : fix lis3l02dq , read error handling
drm : fixup improper cast .
staging : media : lirc : Use pr_ * instead of printk
drm / i915 : Only clear the GPU domains upon a successful finish
staging : dwc2 : when dma is disabled , clear hcd -> self . uses_dma
[ IB ] mthca : fix off by one in clr_int calculation
i2c / ds1374 : Check workqueue creation status
mmc : dw_mmc : Don ' t print data errors
rtc : rtc - twl : Switch to using threaded irq
pinctrl : fix a minor harmless typo
i4l : leak in eicon / idifunc . c
bnx2 : Zero out context memory for 5709 .
mm : node_states : introduce N_MEMORY
staging : sm750fb : fix style for if clause
mm : fix NULL ptr deref when walking hugepages
iwlwifi : mvm : BT Coex - fix shared antenna check with new API
ceph : add missing locking to protect i_snap_realm_item during split
[ ETH ]: Validate address in eth_mac_addr
s390 / hugepages : clear page table for sw large page emulation
ext4 : Make sure the MOVE_EXT ioctl can ' t overwrite append - only files
igb : limit EEPROM access
ASoC : wm9713 : fix regmap free path
intel - iommu : Fix reattaching of devices to identity mapping domain
fix a race in ks8695_poll
xfrm : Force SA to be lookup again if SA in acquire state
[ PATCH ] IB : Initialize qp -> wait
KVM : x86 emulator : fix lea to really get the effective address
ASoC : wm8994 : Move wm_hubs callback before we start ramping VMID
gpio - 74x164 : Fix buffer allocation size
usb : Do not allow usb_alloc_streams on unconfigured devices
cfg80211 : Process all pending regulatory requests / hints
rt2x00 : Add RT539b chipset support
mtip32xx : remove unneeded variable in mtip_cmd_timeout ()
qeth : Clear SBALF15 in any case for output buffers .
[ PATCH ] bug in futex unqueue_me
ceph : exclude snapdir from readdir results
USB : wusb : check CHID is all zeros before stopping the host
staging : vt6655 : check ieee80211_bss_conf bssid not NULL
[ NETFILTER ]: bridge : fix double POSTROUTING hook invocation
arch / powerpc / sysdev / ehv_pic . c : add missing kfree
sch_gred : should not use GFP_KERNEL while holding a spinlock
drm : Fix a bug in the range manager .
bridge : netfilter : orphan skb before invoking ip netfilter hooks
staging : comedi : fix typo in error message
brcm80211 : smac : set interface down on reset
staging : lustre : info leak in lnet_ping ()
mm : vmalloc search restart fix
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
[ IPV6 ]: Fix ipv6 GSO payload length
pm2fb : reset transparency settings
iwlwifi : fix situation in which debug message is printed
bsg : fix bsg_poll () to return POLLOUT properly
[ IA64 ] Fix trap # 45 handling
[ SCSI ] ps3rom : sector size should be 512 bytes
staging : vt6655 : dpc . c : replace memcpy () by ether_addr_copy () using coccinelle
davinci_emac : off by one
[ PATCH ] lockdep : annotate scheduler runqueue locks
m68k : serial167 missing return value in cy_put_char ()
drivers / net / phy / mdio_bus . c : call put_device on device_register () failure
[ XFRM ]: xfrm_address_t annotations
Staging : cxt1e1 : use netdev_priv to fix build
KVM : MCE : Add MCG_SER_P into KVM_MCE_CAP_SUPPORTED
[ ALSA ] Add missing compat ioctls for ALSA control API
Staging : android : timed_gpio : Properly discard invalid timeout values .
rtl8180 : avoid potential NULL deref in rtl8180_beacon_work
vhost / scsi : Check LUN structure byte 0 is set to 1 , per spec
perf timechart : Fix SIBGUS error on sparc64
smsc - ircc2 : Fix section mismatch derived from smsc_ircc_pnp_probe ()
drm / exynos : dsi : modify a error type when getting a node failed
[ PATCH ] Missed error checking for intent ' s filp in open_namei ().
iwlwifi : mvm : Always use SCAN_TYPE_FORCED
mmc : core : Fix use of uninitialized data in mmc_send_if_cond .
staging / sep : Fix smatch false positive about potential NULL dereference in sep_main . c
GFS2 : fsck . gfs2 reported statfs error after gfs2_grow
HID : hid - picolcd : Fix memory leak in picolcd_debug_out_report ()
uglify while_each_pid_task () to make sure we don ' t count the execing pricess twice
drm / i915 : Fix SRC_COPY width on 830 / 845g
xen / p2m : Fix one - off error in checking the P2M tree directory .
lockdep : fix debug_show_all_locks ()
hwmon : ( hih6130 ) Fix missing hih6130 -> write_length setting
[ media ] tvp514x : Fix double free
sparc32 : Need to close openned RTC device just like sparc64 .
[ media ] si2157 : advertise Si2158 A20 firmware
md : fix bug in handling of new_data_offset
[ PATCH ] struct path : convert openpromfs
mtd : nand : update mtd_to_nand ()
dmaengine : bcm2835 - dma : Fix memory leak when stopping a running transfer
brcmsmac : handle non pci in ai_deviceremoved ()
vfs : do_last (): only return EISDIR for O_CREAT
tuntap : set SOCK_ZEROCOPY flag during open
spi_bfin : use more useful GPIO labels
[ SCSI ] More USB deadlock fixes
phy layer : fix genphy_setup_forced ( don ' t reset )
staging : csr : remove unneeded call to memset ().
staging : rtl8723au : Fix the indentation of two lines
[ POWERPC ] arch / powerpc / sysdev / qe_lib of_node_get cleanup
[ PATCH ] Mobil Pentium 4 HT and the NMI
[ BNX2 ]: Update version to 1 . 6 . 8 .
xfs : Fix quota type in quota structures when reusing quota file
amdkfd : use atomic allocations within srcu callbacks
stmmac : fix check for phydev being open
btrfs : fix dip leak
drm : copy the right data back to userspace for getreserved contexts ioctl
ASoC : Fix off - by - one bug in WM8962 register cache size configuration
i2c : mv64xxx : continue probe when clock - frequency is missing
gpio : em : Setup gpiochip -> dev
[ PATCH ] ARM SMP : Mark mroe CPU init data with __cpuinitdata
[ PATCH ] myri10ge : update driver version to 1 . 1 . 0
alpha : Fix uninitialized value in read_persistent_clock .
leds : Fix wrong loop direction on removal in leds - ams - delta
HID : multitouch : force retrieving of Win8 signature blob
cifs : Read buffer overflow
mfd : Use devm_ * APIs for mc13xxx : remove leftover kfree
[ IA64 ] Remove sparse warning from unwind code
vxge : Fix a possible memory leak in vxge_hw_device_initialize ().
perf script : Fix a NULL pointer dereference
bridge : mcast snooping , fix length check of snooped MLDv1 / 2
sky2 : Call dev_kfree_skb_any instead of dev_kfree_skb .
[ PATCH ] sys_ioprio_set : minor do_each_thread + break fix
ath9k_hw : index out of bounds
mac80211 : send control port protocol frames to the VO queue
staging : lustre : Remove unnecessary parentheses .
KVM : x86 : apply kvmclock offset to guest wall clock time
usb : gadget : storage : release superspeed descriptors .
tcp : remove unnecessary tcp_sk assignment .
tracing : Have trace buffer point back to trace_array
[ ALSA ] soc - Report errors from snd_soc_dapm_set_endpoint ()
[ MIPS ] Sibyte : Fix name of clocksource .
cifs : ensure that vol -> username is not NULL before running strlen on it
fsnotify : kzalloc fsnotify groups
tipc : fix out of bounds indexing
mac80211 : fix potentially redundant skb data copying
HID : egalax : Use kzalloc
mmc : sdhci - esdhc - imx : change default watermark level and burst length
drm / i915 : Drop any active reference before unbinding
bpf : fix BPF_MAP_LOOKUP_ELEM command return code
USB : atmel_usba_udc : fix freeing irq in usba_udc_remove ()
ath9k : make sure main_rssi is positive
Staging : crystalhd : use vfree () instead of kfree ()
svcrdma : RDMA support not yet compatible with RPC6
memcg : fix leak of accounting at failure path of hugepage collapsing
mfd : add support for WM8351 revision B
iscsi class : fix get_host_stats error handling
[ MIPS ] add io_map_base to pci_controller on Cobalt
ARM : 7803 / 1 : Fix deadlock scenario with smp_send_stop ()
usb : isp1760 : check for null return from kzalloc
tcp : tcp_disconnect () should clear window_clamp
net : make ipv6 bind honour freebind
[ media ] v4l : vsp1 : Fix VI6_DISP_IRQ_ENB_LNEE macro
[ SCSI ] fc_transport : Check portstates before invoking target scan
tty / serial : at91 : correct buffer size used in DMA
staging : ozwpan : Increase interrupt end point buffer size
fs : Fix theoretical division by 0 in super_cache_scan ().
[ PATCH ] KVM : fix bogus pagefault on writable pages
[ SPARC64 ]: Fix pfn_pte () build failure .
ext4 : use the correct error exit path in ext4_init_inode_table ()
i386 : arch / x86 / math - emu / fpu_entry . c warning fix
sfc : Clear I2C adapter structure in falcon_remove_nic ()
[ ATA ] Add named constant for ATAPI command DEVICE RESET
ALSA : snd - usb : Fix uninitialized variable usage
[ SCSI ] ipr : Bump version
[ S390 ] fix initialization of stp
ceph : fscache : Update object store limit after file writing
iwlwifi : unlock on error path
perf inject : Do not repipe attributes to a perf . data file
fib6 : use FIB_LOOKUP_NOREF in fib6_rule_lookup ()
KVM : Fix coalesced interrupt reporting in IOAPIC
bonding : always update last_arp_rx on packet recieve
ceph : drop CAP_LINK_SHARED when sending " link " request to MDS
perf record : Use sw counter only if hw pmu is not detected
Staging : wlan - ng : fix style in p80211netdev . c
perf / x86 / amd : Fix RDPMC index calculation for AMD family 15h
[ PATCH ] uml : const a variable
[ media ] hdpvr : Fix memory leak
x86 : Use string instruction memcpy on AMD Fam11h
char / tpm : Fix unitialized usage of data buffer
viafb : reduce I2C timeout and delay
cxl : Plug irq_bitmap getting leaked in cxl_context
[ MIPS ] Fix oprofile module unloading
drm / amdkfd : unmap VMID <--> PASID when relesing VMID ( non - HWS )
x86 , efi : Check table header length in efi_bgrt_init ()
omap3 : Fix booting if package is uninitialized
mm : memblock : fix wrong memmove size in memblock_merge_regions ()
Input : ab8500 - ponkey - fix IRQ freeing in error path
cfg80211 : enter psm when working as p2p_cli
ixgbe : reinit_locked () should be called with rtnl_lock
misc : genwqe : fix return value check in genwqe_device_create ()
md : check the return of mddev_find ()
ARM : shmobile : r8a7778 : add MMCIF clock support for DT
ARM : mach - imx : mach - imx6q : Fix sparse warnings
mac80211 : pass AP vif pointer for VLANs
Btrfs : fix false positive in check - integrity on unmount
[ ALSA ] system timer : clear correction value when timer stops
[ PATCH ] USB : fix gadget_is_musbhdrc ()
e1000e : potentially incorrect return for e1000e_setup_fiber_serdes_link
adm8211 : fix cfg80211 band API conversion
PCI : fix bridge I / O window bus - to - resource conversion
ipv6 : Export nf_ct_frag6_gather ()
arm64 / BUG : Show explicit backtrace for WARNs
[ PATCH ] m68k : isa_ { type , sex } should be exported
um : finally kill -> init_str leaks
Staging : speakup : free after input_unregister_device ()
staging : zcache : fix possible sleep under lock
[ PATCH ] x86_64 : free_bootmem_node needs __pa in allocate_aperture
ima : Do not free ' entry ' before it is initialized
ASoC : wm8731 : fix wm8731_check_osc () connected condition
[ media ] V4L : mt9t112 : use after free in mt9t112_probe ()
perf_events : Fix invalid pointer when pid is invalid
ALSA : Fix a Oops bug in omap soc driver .
ALSA : usb - audio : Fix format string mismatch in mixer . c
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
drbd : fix if (); found by kbuild test robot
ipv6 : fix headroom calculation in udp6_ufo_fragment
leds : 88pm860x : add missing of_node_put
libceph : init event -> node in ceph_osdc_create_event ()
virtio_net : zero is an invald queue_pairs number
ARM : mx51 : Fix the address space length for SSI
cxgb4 : allow large buffer size to have page size
drm / msm : fix memory leak
net : make all team port device link events urgent
[ SCSI ] libfc : Fix e_d_tov ns -> ms scaling factor in PLOGI response .
md : avoid possible spinning md thread at shutdown .
usb - storage : Add 090c : 1000 to unusal - devs
dm raid1 : switch rh_in_sync to blocking in do_reads
staging : ozwpan : prevent overflow in oz_cdev_write ()
mfd : wm8994 : Silence bogus warning in wm8994_device_init ()
ath9k_hw : add default chainmask for AR9462
spi : s3c64xx : Zero dma_slave_config struct in prepare_dma ()
btrfs : don ' t BUG_ON allocation errors in btrfs_drop_snapshot
stmmac : fix csr clock divisor for 300MHz
sky2 : use roundup () macro
iucv : fix memory leak in cpu hotplug error path .
/ drivers / staging / dgrp : Fixed warning unchecked sscanf return value
ASoC : generic - dmaengine - pcm : use a more common dma name
KVM : ensure that memslot userspace addresses are page - aligned
ALSA : sb16 - info leak in snd_sb_csp_ioctl ()
[ media ] gspca - ov519 : Fix a regression for ovfx2 webcams
drm / nouveau / core : drop the pointer value in debug printk output
sh : intc : Allocate subgroup virq backing desc directly .
cifs : fix off - by - one bug in build_unc_path_to_root
atmel_usba_udc : Add missing kfree () in usba_udc_remove ()
sunrpc : copy scope ID in __rpc_copy_addr6
drm / ttm : Clear the ghost cpu_writers flag on ttm_buffer_object_transfer .
igbvf : clear buffer_info -> dma after dma_unmap_single ()
tg3 : remove open - coded skb_cow_head .
[ ALSA ] soc - 0 . 13 ASoC DAPM bug fix for unnamed streams
ext3 : explicitly remove inode from orphan list after failed direct io
[ media ] s5p - fimc : Fix return value of __fimc_md_create_flite_source_links ()
GFS2 : write_end error path fails to unlock transaction lock
arm64 : drop unnecessary cache + tlb maintenance
bnx2x : fix DMAE timeout according to hw specifications
[ MIPS ] Sibyte : Fix interrupt timer off by one bug .
mwifiex : use maximum ssid length as 0xfe for p2p
[ IA64 ] mbcs_init () should give up unless running on sn2
mtd : cmdlinepart : allow small partitions
drm / i915 : switch to default context on idle
fuse : fix d_revalidate oopsen on NFS exports
Btrfs : Don ' t case unsigned long to int in bio submission
gpio / ep93xx : Use irq_set_handler_locked ()
spi : img - spfi : Enable controller before starting TX DMA
[ NETFILTER ]: Remove okfn usage in ip_vs_core . c
sh64 : re - add the __strnlen_user () prototype
drm / exynos : exynos_hdmi : Pass correct pointer to free_irq ()
rt2x00 : fix a wrong parameter for __test_and_clear_bit () in rt2x00rfkill_free ().
kdb : break out of kdb_ll () when command is terminated
clk : make sure parent is not NULL in clk_fetch_parent_index ()
Phonet : fix double free in GPRS outbound packet error path
perf tools : Disallow PMU events intel_pt and intel_bts until there is support
md : allow a partially recovered device to be hot - added to an array .
HID : wacom : Do not repeatedly attempt to set device mode on error
sched : Disallow sched_attr :: sched_policy < 0
video : udlfb : Use NULL instead of 0
powerpc / vphn : NUMA node code expects big - endian
cpufreq : e_powersaver : call CPUFREQ_POSTCHANGE notfier in error cases
perf ui hist browser : Fix segfault on ' a ' for annotate
[ PATCH ] key : plug request_key_auth memleak
tracing : Fix event format export
drivers / char / hpet . c : fix information leak to userland
cciss : handle allocation failure
ext4 : check free block counters in ext4_mb_find_by_goal
gpio : adp5588 : remove unnecessary OOM messages
usb : phy : ab8500 - usb : fix phy tuning value select logic
isdn : avoid copying too long drvid
drm : drm_ioctl () should zero - init extra data
ALSA : hda / realtek - Create " Bass Speaker " for two speaker pins
macb : Fix speed setting
[ media ] em28xx : fix em28xx - input removal
ASoC : fsi : fixup pm_runtime_disable () timing on fsi_probe ()
mfd : t7l66 MMC platform data
arch / powerpc / sysdev / qe_lib / qe . c : Add of_node_put to avoid memory leak
usb : gadget : fusb300_udc : fix error return code in fusb300_probe ()
ASoC : max9867 : silence and array overflow warning
drm / radeon / kms : fix typo in spread spectrum code
KVM : Fix buffer overflow in kvm_set_irq ()
Btrfs : btrfs_add_ordered_operation : Fix last modified transaction comparison .
[ SCSI ] gdth : fix buffer overflow
PCI : eliminate double kfree in intel - iommu initialization
ASoC : Staticise pxa2xx_pcm_ops
[ PATCH ] libata : kill illegal kfree ( id )
ASoC : arizona : Store the DAI clock ID when setting
gpio : delete unneeded test before of_node_put
tracing / ftrace : make nop tracer reset previous entries
x86 / PCI : move final fixups from __init to __devinit
ARM : Fix warning : unused variable ' highmem '
[ PATCH ] readahead : reset cache_hit earlier
tcp : skb_mstamp_after helper
cris : fix potential NULL - pointer dereference
mmc : card : Avoid null pointer dereference
dmaengine : ste_dma40 : fix error return code
ARM : OMAP2 +: only WARN if a GPMC child probe function fail
ASoC : wm8995 : Return - EINVAL if device ID mismatch
firewire : fix failure path in ohci_enable_phys_dma
rt2800usb : initialize H2M_INT_SRC register
[ SCTP ]: Validate the parameter length in HB - ACK chunk .
USB : serial : fix up bug with missing {}
netrom : Fix nr_getname () leak
ipv4 : warn once on passing AF_INET6 socket to ip_recv_error
mac80211 : add status_driver_data array to ieee80211_tx_info
USB : whiteheat : fix memory leak in error path
ath9k : fix massive rx packet loss issue
sched : add sanity check in partition_sched_domains ()
wl12xx : update PLT initialization for new firmware
drivers / net / ax88796 . c : Return error code in failure
ext4 : error proc entry creation when the fs / ext4 is not correctly created
iwlwifi : mvm : initialize the cur_ucode upon boot
[ PATCH ] USB : check for device in zd1201_resume
staging : wilc1000 : off by one in wilc_wfi_cfg80211_mgmt_types
firewire : cdev : fix another memory leak in an error path
pinctrl : lpc18xx : fix schmitt trigger setup
sound : fix check for return value in snd_pcm_hw_refine
[ PATCH ] sysfs : fix a kobject leak in sysfs_add_link on the error path
drm_vblank_get : don ' t WARN_ON in case vblanks are not initialized
usb : gadget : f_printer : actually limit the number of instances
ath9k : Clear HT info on starting the driver
staging : rts_pstor : Complete scanning_done variable
GFS2 : quotas not being refreshed in gfs2_adjust_quota
gpio / samsung : Complain loudly if we don ' t know the SoC
af_unix : implement socket filter
wl18xx : create per - chip - family private storage
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
gro : Fix bogus gso_size on the first fraglist entry
[ PATCH ] orinoco : Make nortel_pci_hw_init () static .
drivers / char / applicom . c : fix information leak to userland
Staging : lustre : osc : Remove null check before kfree
md : add takeover support for raid4 -> raid5 conversion .
hsi : nokia - modem : fix uninitialized device pointer
drm / nouveau / fifo / nvc0 : bash some magic reg to make uevent interrupt work
ext4 : add missing space to ext4_msg output in ext4_fill_super ()
Btrfs : remove dead code
ALSA : hda / realtek - Add more entry for enable HP mute led
[ IA64 - SGI ] Do not request DMA memory for BTE
x86 / nmi : export local_touch_nmi () symbol for modules
ALSA : hda - check NULL pointer when creating SPDIF controls
arm / arm64 : KVM : fix missing unlock on error in kvm_vgic_create ()
[ media ] solo6x10 : Fix solo_eeprom_read retval type
mwifiex : fix Coverity SCAN CID 709078 : Resource leak ( RESOURCE_LEAK )
simple_xattr : permit 0 - size extended attributes
x86 , mrst : Don ' t blindly access extended config space
ath10k : make sure to use passive scan when n_ssids is 0
Btrfs : ignore non - FS inodes for send / receive
bnx2x : Prevent " Unknown MF " print in SF mode
drm : Revert " drm : Reject modes with more than 1 stereo flags set "
[ SCSI ] pmcraid : reject negative request size
mmc : sdhci - bcm - kona : Handle error from mmc_of_parse ()
ARM : tegra : fix U16 divider range check
bonding : bond_xmit_roundrobin () fix
drivers / staging / hv / blkvsc_drv . c : eliminate NULL pointer dereference
staging : gdm72xx : Condense two statements into one to improve code readability .
be2net : initialize netdev -> perm_addr
dma / timberdale : simplify conditional
sched / balancing : Fix ' local -> avg_load > sds -> avg_load ' case in calculate_imbalance ()
mlx4_en : Fix read buffer overflow in mlx4_en_complete_rx_desc ()
drm / i915 : Check the framebuffer offset
amd - iommu : set evt_buf_size correctly
rt2x00 : Fix memleak when RTS / CTS fails
rt2x00 : Shortcut link state updates when not operating as STA
staging : ozwpan : Check for correct config number .
ipvs : WRR scheduler does not need GFP_ATOMIC allocation
[ media ] cx25821 - audio - upstream . c : Move operators
sfc : Read - to - clear LM87 alarm / interrupt status at start of day
iscsi - target : return - EINVAL on oversized configfs parameter
drm / vmwgfx : Fix nasty write past alloced memory area
[ IPV6 ] ADDRCONF : Register inet6_dev earlier .
[ media ] marvell - cam : Don ' t signal multiple frame completions in scatter / gather mode
HWPOISON : Fix page count leak in hwpoison late kill in do_swap_page
[ PATCH ] IB : fix CM use - after - free
perf tools : Fix new_term () missing free on error path
md : skip all metadata update processing when using external metadata .
[ media ] pvrusb2 : Fix a few missing default control values , for cropping
batman - adv : Fix mem leak in the batadv_tt_local_event () function
ath9k : set MAC version for AR9550
usb : core : check for valid id_table when using the RefId feature
i386 : Handle P6s without performance counters in nmi watchdog
ath9k : check for NULL rate when using ieee80211_get_rts_cts_rate ()
ixgbe : support software timestamping
[ X25 ]: Restore skb -> dev setting in x25_type_trans ().
KVM : ioapic : fix assignment of ioapic -> rtc_status . pending_eoi ( CVE - 2014 - 0155 )
drm / i915 : Reject opening of pipe crc files for invalid pipes
vfio : fix crash on rmmod
rcu : Export rcu_scheduler_active
drm / i915 : fix inconsistent brightness after resume
vxge : Update driver_config -> vpath_per_dev for each function in probe .
Btrfs : process mount options on mount - o remount ,
usb : dwc3 : gadget : add a debugging print when initializing endpoints
UBI : init_volumes : Ignore volumes with no LEBs
ceph : avoid null deref in osd request error path
ASoC : wm5110 : Correct DSP4R Mixer control name
drm / i915 : Fix missing return warning for ! CONFIG_DEBUGFS
sh_eth : check TSU registers ioremap () error
ASoC : tlv320aic3x : Convert codec -> hw_read to snd_soc_read
mfd : Copy the device pointer to the twl4030 - madc structure
iwlagn : overwrite EEPROM chain setting for 6250 devices
[ PATCH ] selinux : Fix MLS compatibility off - by - one bug
[ SCSI ] virtio_scsi : fix memory leak on full queue condition .
Input : turbografx - fix potential out of bound access
[ media ] cx24120 : Add missing command to cx24120_check_cmd
[ POWERPC ] EEH : Work with device endpoint , always
Input : ALPS - fix command mode check
x86 , tsc : Fix cpufreq lockup
[ SPARC64 ]: Fix loop termination in mark_kpte_bitmap ()
ARM64 : early_printk : Fix check for CONFIG_ARM64_64K_PAGES
[ media ] v4l : vb2 - dma - contig : align buffer size to PAGE_SIZE
pinctrl : handle zero found dt pinconfig properties better
leak in do_ubd_request
tcp_yeah : don ' t set ssthresh below 2
mac80211 : use RCU read locks for sta_info_get
/ proc / vmcore : fix seeking
mm : fix lost kswapd wakeup in kswapd_stop ()
thermal : core : ignore invalid trip temperature
rtlwifi : rtl8192cu : Fix error in pointer arithmetic
[ PATCH ] zd1211rw : Add ID for Belkin F5D7050 v4000
mnt : Carefully set CL_UNPRIVILEGED in clone_mnt
KVM : async_pf : Add missing call for async page present
xhci : Handle HS bulk / ctrl endpoints that don ' t NAK .
gpio : potential null dereference
mmc : atmel - mci : r / w proof capability only available since v2xx
gpio : Fix range check in of_gpio_simple_xlate ()
kvm / x86 : Remove Hyper - V SynIC timer stopping
USB : chaoskey read offset bug
crypto : mxs - dcp - fix type of ret for wait_for_completion_timeout
um : Ensure that a stub page cannot get unmapped
drm / amdgpu : fix bug of vm_bo_map ( v2 )
sparc : semtimedop () unreachable due to comparison error
ARM : EXYNOS : Recognize Samsung MFC v8 devices
at86rf230 : cleanup and squash stack variable
[ PATCH ] Fix RLIMIT_RTPRIO breakage
drm / nouveau / bios : check for null script pointers in parser
drivers / video / backlight / adp8870_bl . c : add missed props . type conversion
drivers / staging / lustre : Fix set - but - unused whinge .
tracing : stop stack trace on first empty entry
tty : moxa : fix bit test in moxa_start ()
drm / vmwgfx : Fix uninitialized return in vmw_cotable_unbind ()
staging : vt6655 : buffer overflow in ioctl
sh_eth : Fix memleak in sh_mdio_release
ext3 : Truncate allocated blocks if direct IO write fails to update i_size
x86 : tsc_64 . c make constant UL
[ SCSI ] mpt2sas : remove extraneous sas_log_info messages
hwmon / smsc47b397 : Don ' t report missing fans as spinning at 82 RPM
cfg80211 : DFS check dfs_region before usage
mmc : mxs - mmc : fix error return code in mxs_mmc_probe ()
tridentfb : fix hang on Blade3D with CONFIG_CC_OPTIMIZE_FOR_SIZE
ASoC : Fix typo in 24 . 576MHz rate in WM5100
iio : Accept a leading '+' sign when parsing fixed point numbers
irq : check domain hwirq range for DT translate
[ TCP ]: Never TSO defer under periods of congestion .
ALSA : compress_core : integer overflow in snd_compr_allocate_buffer ()
mm : vmalloc allocator off by one
drm / i915 : Simplify i915_gem_obj_is_pinned () test for set - tiling
[ PATCH ] eCryptfs : dput () lower d_parent on rename
fbcon : bgcolor fix
powerpc / ipic : introduce missing kfree
mac80211 : make sure hw_key exists before checking its flags
kmemleak : Fix the kmemleak tracking of the percpu areas with ! SMP
l2tp : fix a potential skb leak in l2tp_xmit_skb ()
be2iscsi : add an missing goto in error path
Staging : unisys : visorinput : Remove unused code
[ PATCH ] inotify : fix idr_get_new_above usage
[ PATCH ] savagefb : Fix kfree before use
kexec : fix omitting offset in extended crashkernel syntax
Btrfs : make error return negative in btrfs_sync_file ()
ASoC : rsnd : rsnd_dai_connect () returns error if it connect to existing mod
Btrfs : Fix a crash when mounting a subvolume
mtip32xx : Set queue bounce limit
KVM : MMU : Fix cleaning up the shadow page allocation cache
blkcg : don ' t call into policy draining if root_blkg is already gone
dm snapshot : lock snapshot while supplying status
ARM : OMAP1 : omap h2 regression fix
x86 / PCI : make sure _PAGE_IOMAP it set on pci mappings
fealnx : Write outside array bounds
svcrdma : Free context on post_recv error in send_reply
ovl : fix open in stacked overlay
cgroup : use release_agent_path_lock in cgroup_release_agent_show ()
irda : off by one
ixgbe : check for vfs outside of sriov_num_vfs before dereference
ALSA : cs4232 : fix crash during chip PNP detection
sdio : kick the interrupt thread upon a resume
staging : binder : fix usage of uninit scalar in binder_transaction ()
ipv6 - multicast : Fix memory leak in input path .
OpenRISC : Export dump_stack ()
x86 / irq : Fix move_irq_desc () for nodes without ram
ftdi_sio : fix kref leak
wl1271 : exit ELP mode when setting enabled rates in tx
staging : ft1000 : dont cast void * from kmalloc ()
b43 : Force - wake queues on init
arm : kvm : fix CPU hotplug
intel_pmic_gpio : fix off - by - one value range checking
rt2x00 : disable TKIP on USB
ide : unexport ide_hwifs []
Staging : sep : fix time handler
staging : rtl8188eu : Use kcalloc instead of kzalloc .
[ PATCH ] remove unnecessary blk_queue_bounce in SG_IO
perf hists : Don ' t consider filtered entries when calculating column widths
xhci : fix off by one error in TRB DMA address boundary check
sctp : Clear fast_recovery on the transport when T3 timer expires .
netrom : fix info leak via msg_name in nr_recvmsg ()
drm / cirrus : Constify function pointer structs
fs / ramfs / file - nommu . c : replace count * size kzalloc by kcalloc
staging : vt6656 : don ' t return zero on failure path in vt6656_probe ()
[ media ] media - device : fix infoleak in ioctl media_enum_entities ()
ethtool : time to blink provided in seconds not jiffies
[ SCSI ] Return NEEDS_RETRY for eh commands with status BUSY
staging , media , easycap : Fix mem leak in easycap_usb_probe ()
econet : Fix econet_getname () leak
hugetlbfs : fix i_blocks accounting
i40e : Increase PF reset max loop limit
um : Missing pipe handling
ath : Fix NULL ptr dereference in ath_reg_apply_world_flags
sigtimedwait : use freezable blocking call
ir - core : Fix the delete logic
ipw2200 : support ETHTOOL_GPERMADDR
brcmfmac : Fix crash on cleanup .
[ TG3 ]: Update version to 3 . 85
[ MTD ] [ OneNAND ] omap2 : panic_write may be in an interrupt context
xen : correct error for building p2m list on 32 bits
VFIO : platform : clear IRQ_NOAUTOEN when de - assigning the IRQ
staging : iio : adc : max1363 : prevent buffer overflow
KVM : Force vmexit with virtual interrupt delivery
xhci : fix sparse warning in xhci - trace . h
dm - raid : silence compiler warning on rebuilds_per_group .
drm / exynos / fbdev : set smem_len for fbdev
sonice : Fix build due to botched netdev_alloc_skb () conversion .
af_packet : remove unnecessary BUG_ON () in tpacket_destruct_skb
sh : Account for INITIAL_JIFFIES when using jiffies clocksource .
crypto : ccm - Fix handling of null assoc data
drivers / cdrom / gdrom . c : fix device number leak
perf : Fix error return code
staging : vt6656 : turn radio off after mac registered .
perf tools : Fix TUI helpline output
[ PATCH ] scsi_error thread exits in TASK_INTERRUPTIBLE state .
[ SCSI ] aacraid : fix potential panic in thread stop
mac80211 : fix PS vs . scan race
chipidea : error on overflow for port_test_write
ARM : 8544 / 1 : set_memory_xx fixes
[ SCSI ] scsi_lib : Set the device state from transport - offline to running
drm / i915 : Missed lock change with rps lock
ata : fix sparse warning in ata_piix . c
usb : host : xhci : Fix Null pointer dereferencing with 71c731a for non - x86 systems
ixgbe : fix for race with 8259 ( 8 | 9 ) during shutdown
x86 / smpboot : Move data structure to its primary usage scope
Input : wistron_btns - fix a memory leak in wb_module_init error path
cfq - iosched : slice offset should take ioprio into account
staging : iio : iio_ring_rip_outer return immediately if rip_lots returns <= 0
tg3 : Turn off ASF " driver alive " heartbeats for APE
[ media ] zoran : fix compiler warning
intel_sst : ignore IRQ when suspended
[ POWERPC ] Fix harmless typo
omap_hsmmc : add missing probe handler hook
[ SCSI ] scsi_transport_iscsi : make priv_sess file writeable only by root
KVM : SVM : Advance instruction pointer in dr_intercept
dt : protect against NULL matches passed to of_match_node ()
UDF : Close small mem leak in udf_find_entry ()
leds : Make header variable naming consistent
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
[ IA64 ] Use set_cpus_allowed_ptr
drm / i915 : intel_frontbuffer_flip_prepare () doesn ' t need struct_mutex
[ XFS ] Wait for all I / O on truncate to zero file size
ALSA : hda - Don ' t refer ELD when unplugged
include / linux / fs . h : fix userspace build
dm mpath : do not fail paths after integrity errors
[ media ] m5mols : Fix bug in stream on handler
e100 : make sure vlan support isn ' t advertised on old adapters
Btrfs : check return value of btrfs_search_slot properly
ALSA : hdsp : silence and underflow warning
ALSA : hda - Detect non - SPDIF digital I / O
b43 : Fix swatch warning
Drivers : hv : hv_balloon : correctly handle val . freeram < num_pages case
regmap : Fix ' ret ' would return an uninitialized value
ipv4 : Limit size of route cache hash table
mx3fb : fix regression with uninitalized fb_info -> mm_lock mutex
ALSA : hda / jack - Fix the assignment of input jack - type
xfrm6 : release dev before returning error
perf stat : better error message for unsupported events
jfs : fix d_revalidate oopsen on NFS exports
sky2 : print Optima chip name
igb : Avoid uninitialized advertised variable in eee_set_cur
[ S390 ] Fix timeval regression on s390
iio : buffer : Free active scan mask in iio_disable_all_buffers ()
NVMe : End sync requests immediately on failure
rtc : Fix ioctl error path return
staging : comedi : disallow COMEDI_DEVCONFIG on non - board minors
hwmon : ( emc1403 ) Remove unnecessary hwmon_device_unregister
ASoC : Check return value of strict_strtoul () in WM8962
cpufreq : pcc : call CPUFREQ_POSTCHANGE notfier in error cases
AFS : Potential null dereference
spidernet : fix memory leak in spider_net_stop
fuse : fix retrieve length
misc : genwqe : Fix potential memory leak when pinning memory
perf hists browser : Fix segfault if use symbol filter in cmdline
[ ARM ] fix leak in iop13xx / pci
[ PATCH ] Char : tty , delete wake_up_interruptible after tty_wakeup
kernel / sys . c : fix missing rcu protection for sys_getpriority ()
[ XFS ] Fix a mutex_destroy diagnostic about a locked - mutex - on - destroy from
drm / ttm : Don ' t clear page metadata of imported sg pages
xfs : check if device support discard in xfs_ioc_trim ()
f2fs : report - ENOENT for unreached data indices
vmwgfx : return - EFAULT if copy_to_user fails
amd - xgbe - phy : Fix build break for missing declaration
brcmfmac : initialize escan function pointer during scheduled scan
f2fs : avoid unneeded lookup when xattr name length is too long
ocfs2 : copy fs uuid to superblock
dmaengine : mv_xor : fix big endian operation in register mode
mmc : omap_hsmmc : Update error code for response_busy cmd
headers_check fix : linux / bfs_fs . h
iwlagn : block regular host commands if driver don ' t own uCode
ext4 : truncate_inode_pages () in orphan cleanup path
sfc : Use kzalloc () to ensure struct efx_spi_device is fully initialised
Btrfs : free tmp ulist for qgroup rescan
usb : ehci - mxc : check for pdata before dereferencing
drm / i915 : WARN if the bios reserved range is bigger than stolen size
usb : dwc3 : ep0 : move to CONFIGURED also on delayed status
[ PATCH ] IB / mthca : allocate correct number of doorbell pages
cfq - iosched : Respect ioprio_class when preempting
mm : check that we have the right vma in __access_remote_vm ()
ide : add missing rq . ref_count initialization to ide_diag_taskfile ()
gpio - mcp23s08 : correctly handling failed allocation
xtensa : fix sysmem reservation at the end of existing block
writeback : laptop_mode_timer_fn () needs rcu_read_lock () around bdi_writeback iteration
platform - drivers - x86 : intel_menlow : add missing return AE_OK for intel_menlow_register_sensor ()
[ PATCH ] zd1211rw : Add ID for Sitecom WL - 162
drivers / net / wan : introduce missing kfree
x86 : BUILD_IRQ say . text to avoid . data . percpu
[ PATCH ] compat_sys_nfsservctl (): handle errors correctly
toshiba_acpi : Fix mis - merge
pinctrl : baytrail : add missing module removal support
perf tools : Try to lookup kernel module map before creating one
Staging : unisys : Use kcalloc instead of kzalloc to allocate array
mac80211 : fix memory leak
drm / nouveau : No need to force evict = true when swapping evicted BOs back in .
f2fs : enable fast symlink by utilizing inline data
ab8500 - fg : Use correct battery charge full design
rtc - at91rm9200 : add match - table compile guard
staging : hfi1 : sdma : Use setup_timer
[ SCSI ] ibmvfc : Fix invalid error response handling
mwl8k : Disable tx_wait completion
ALSA : asihpi - Fix outstream start trigger for non - mmap adapters .
[ ALSA ] hda - codec - Add first generation macbook subsystem ID
drm / i915 / crt : Flush register prior to waiting for vblank .
spi_mpc8xxx : Fix uninitialized variable
crypto : pcrypt - Use crypto_aead_set_reqsize helper
clk : shmobile : div6 : Avoid division by zero in . round_rate ()
ceph : fix ack counter reset on connection reset
fs_enet : Fix a memory leak in fs_enet_mdio_probe
hub : debug message for failing to enable device
cpufreq : powernv : Free ' chips ' on module exit
iio : accel : bmc150 - accel : fix counting direction
[ XFS ] fix valid but harmless sparse warning
esas2r : fix an error path in esas2r_ioctl_handler
raid5 : disable batch with log enabled
fuse : reset waiting
clk : ti : apll : not allocating enough data
hwmon : ( ntc_thermistor ) Ensure that data -> name string is terminated
ath9k : Fix rx filtering issue for older chips
mac80211 : Fixing slow IBSS rejoin
tracing / filters : free pred when clearing filters
acer - wmi : support Lenovo ideapad S205 1038DPG wifi switch
ASoC : s6000 : Fix unlocked snd_pcm_stop () call
ACPI : Change package length error to warning
UBI : add missing kmem_cache_free () in process_pool_aeb error path
[ XFRM ]: Fix possible overflow of sock -> sk_policy
ALSA : bebob : Fix a missing to unlock mutex in error handling case
decnet : fix shutdown parameter checking
drm / i915 : Skip stolen region initialisation if none is reserved
staging : comedi : do not return - ENOSYS .
drm : sti : Fix return value check in sti_drm_platform_probe ()
audit : Handle embedded NUL in TTY input auditing
amd8111e : Don ' t receive packets when the napi budget == 0
um : don ' t leak floating point state and segment registers on execve ()
batman - adv : remove broadcast packets scheduled for purged outgoing if
HID : debug : fix error handling in hid_debug_events_read ()
KVM : x86 emulator : VM86 segments must have DPL 3
drivers / rtc / rtc - pcf2123 . c : fix error return code in pcf2123_probe ()
usb : musb : gadget : clear gadget_driver when gadget is stopped
clk : versatile : free icst on error return
clk : divider : return real rate instead of divider value
[ POWERPC ] Make irq_dispose_mapping ( NO_IRQ ) a nop
drm / radeon : check kmalloc () for failures
UBI : bugfix : dont oops with NULL module parameter
mtd : nand : mxc_nand : Check the return value from clk_prepare_enable ()
spi : mediatek : remove clk_disable_unprepare ()
svcrpc : make svc_create_xprt enqueue on clearing XPT_BUSY
usb : chipidea : udc : . pullup is valid only when vbus is there
mm , hugetlb : add VM_NORESERVE check in vma_has_reserves ()
pcmcia : rsrc_nonstatic : check value , not pointer
iommu / amd : Fix BUG when faulting a PROT_NONE VMA
x86 apic_32 . c section fix
kgdb : add missing __percpu markup in arch / x86 / kernel / kgdb . c
kvm : irqchip : fix memory leak
sd : Clear PS bit before Mode Select .
gianfar : Fix missing sock reference when processing TX time stamps
ASoC : ad1836 : fix a memory leak if another ad1836 is registered
vhost - net : fix vq_memory_access_ok error checking
Input : twl4030_keypad - pass correct pointer to free_irq ()
gpio : max730x : eliminate double free
leds : turn the blink_timer off before starting to blink
mmc : mxcmmc : handle mmc_of_parse () errors during probe
staging : netlogic : Fix multiple assignments
Input : edt - ft5x06 - ignore touchdown events
staging : comedi : ni_nio_common : don ' t write non - existing caldac ' s
staging / rdma / hfi1 : Don ' t attempt to qualify or tune loopback plugs
[ POWERPC ] Fix rtas_flash procfs interface
PCI : host : Add of_pci_get_host_bridge_resources () stub
[ POWERPC ] Fix powermac late initcall to only run on powermac
ath6kl : Fix bug in scheduling hb_timer
[ media ] sony - btf - mpx : Drop needless newline in param description
wl12xx : minor fix in sched_scan_ssid_list
perf python : Initialize ' page_size ' variable
uselib : add missing MNT_NOEXEC check
wlcore : save last regdom configuration on stop
drm / nouveau / pm : don ' t read / write beyond end of stack buffer
[ NETFILTER ]: ip6t_mh : drop piggyback payload packet on MH packets
[ ALSA ] hda - code : Fix the array size of codec list
libata : initialize qc -> dma_dir to DMA_NONE
ALSA : hda - Skip ELD notification during PM process
KVM : PPC : Add emulation for dcba
perf trace / scripting : Check return val of perl_run ()
PCI : Allow a NULL " parent " pointer in pci_bus_assign_domain_nr ()
[ PATCH ] knfsd : nfsd4 : fix open permission checking
ext4 : don ' t save the error information if the block device is read - only
ssb : Fix error when V1 SPROM extraction is forced
cxl : Reset default context for vPHB on release
PCI : Mark 64 - bit resource as IORESOURCE_UNSET if we only support 32 - bit
GFS2 : Fix a use - after - free that coverity spotted
USB : Deref URB after usbmon is done with it
vfs : fix propagation of atomic_open create error on negative dentry
vmalloc : remove vmalloc_sync_all () from alloc_vm_area ()
xfrm : Don ' t allow esn with disabled anti replay detection
ixgbe : Convert the normal transmit complete path to dev_consume_skb_any ()
[ SCSI ] scsi_dh : Check for sdev state in store_dh_state ()
staging : iio : hmc5843 : Set iio name property in sysfs
dmaengine : ste : use DMA_COMPLETE for dma completion status
amd / powerplay : don ' t enable ucode fan control if vbios has no fan table
ath10k : fix error handling in pci_probe
[ SCSI ] qla2xxx : Perform firmware dump procedure on mailbox command timeout .
drm / i915 : Reset vma -> mm_list after unbinding
perf symbols : Properly align symbol_conf . priv_size
[ IPSEC ] Set byid for km_event in xfrm_get_policy
[ media ] media : davinci : vpif_display : drop unused member fbuffers
staging : lustre : prevent underflows in lnet_parse_reply ()
ethtool : Fix potential kernel buffer overflow in ETHTOOL_GRXCLSRLALL
caif - hsi : Add missing return in error path
[ media ] radio - aimslab . c needs # include < linux / delay . h >
qeth : keep ip - address after LAN_OFFLINE failure
drivers / staging / cx25821 / cx25821 - video . c : introduce missing kfree
ext4 : remove dynamic array size in ext4_chksum ()
ALSA : hda / generic - Check power state cap at updating the widget power
dmaengine : omap - dma : Correct status reporting for memcpy
[ S390 ] idle : avoid RCU usage in extended quiescent state
scsi : storvsc : fix SRB_STATUS_ABORTED handling
thinkpad_acpi : Fix a memory leak during module exit
staging : vt6656 : check ieee80211_bss_conf bssid not NULL
cfg80211 : off by one in nl80211_trigger_scan ()
[ PATCH ] ext3 : Wrong return value for EXT3_IOC_GROUP_ADD
KVM : PIT : control word is write - only
USB : fix regression occurring during device removal
devres : WARN () and return , don ' t crash on device_del () of uninitialized device
[ ALSA ] hda - codec - Fix NULL dereference in generic hda code
[ SCSI ] mptfusion : Fix memory leak in mptctl_getiocinfo ()
fix wrong argument of tc35815_read_plat_dev_addr ()
perfcounters / powerpc : fix oops with multiple counters in a group
cfq - iosched : make seek_mean converge more quickly
[ IA64 ] set_thread_area fails in IA32 chroot
ARM : shmobile : r8a7794 : Reserve memory as other R - Car Gen2 SoCs
[ PATCH ] coverity : tty_ldisc_ref return null check
ACPICA : Fix for memory leak related to DdbHandle objects
drivers / net / hamradio / 6pack . c : move a dereference below a NULL test
ALSA : make sparc / dbri . c : snd_dbri_proc () static
[ MTD ] mtd / ofpart . c : add MODULE_LICENSE
drivers : sh : Enable PM runtime for new R - Car Gen2 SoCs
[ PATCH ] Fix sys_migrate_pages : Move all pages when invoked from root
[ PATCH ] ppc64 : lparconfig . c memory leak
[ SCSI ] qla2xxx : Process DPC requests within valid Fabric topologies .
staging : wilc1000 : removes potential null dereference
wlcore : initialize per - link FW freed blocks correctly
staging : comedi : ni_at_ao : fix namespace clash of ' RSI ' define
mm : cma : fix condition check when setting global cma area
w1 : fix for loop exit condition in w1_seq_show ()
firewire : core : remove an unnecessary zero initialization
hso : Add new product ID
ALSA : als4000 : enable burst mode
tcp : Fix tcp_mark_head_lost () with packets == 0
Staging : comedi : fix memory leak in comedi_bond . c
Staging : wlan - ng : memsetting the wrong amount of data
iommu / vt - d : Fix memory leak in dmar_insert_one_dev_info ()
drm / i915 : fix dmabuf vmap support
drm : fixes when i2c encoder slave mode_fixup is null .
[ XFS ] Fix lock inversion in forced shutdown .
ocfs2 : unmap_mapping_range () in ocfs2_truncate ()
[ ARM ] 4557 / 1 : Fix PXA irq gpio initialization
r8169 : balance pci_map / pci_unmap pair
sh : force dcache flush if dcache_dirty bit set .
eeepc - laptop : store_cpufv : return error if set_acpi fails
[ media ] media : omap3isp : fix a potential NULL deref
KVM : nVMX : Report 2MB EPT pages as supported
USB : Fix option_ms regression in 2 . 6 . 31 - rc2
ALSA : hdsp - Ignore MIDI and PCM events in interrupts until initialized
ceph : fix rare potential cap leak
iget : use iget_failed () in GFS2
e1000e : magic number cleanup - ETH_ALEN
[ XFS ] Check return value of xfs_buf_get_noaddr ()
bridge : Clear IPCB before possible entry into IP stack
staging : comedi : das16m1 : use comedi_buf_write_samples ()
perf thread : Allow deletion of a thread with no map groups
i40e : validate VSI param from VFs
[ media ] tda10071 : fix returned symbol rate calculation
ACPI : Fix crash on ASUS laptops
setup_arg_pages : diagnose excessive argument size
perf tools : Set vmlinux_path__nr_entries to 0 in vmlinux_path__exit
staging : gdm72xx : underflow in netlink_rcv_cb ()
net / 9p : Initialize opts -> privport as it should be .
efivarfs : Never return ENOENT from firmware
xfs : suppress spurious uninitialised var warning in xfs_bmapi ()
mac80211 : fix ie memory allocation for scheduled scans
[ media ] pctv452e : Replace memset with eth_zero_addr
drm / i915 / skl : Add missing SKL ids
staging : drm / omap : allocation fail check
xfs : validate acl count
[ PATCH ] make kernel / printk . c : ignore_loglevel_setup () static
x86 : correct the CPUID pattern for MSR_IA32_MISC_ENABLE availability
ALSA : rme32 . c irq enabling after spin_lock_irq
usb : xhci : exit early in xhci_setup_device () if we ' re halted or dying
drm / r600 : fix memory leak introduced with 64k malloc avoidance fix .
fs / proc / task_mmu . c : check the return value of mpol_to_str ()
OMAPDSS : HDMI : init output earlier
ath10k : fix num_legacy_stations tracking
x86 : fix runtime error in arch / x86 / kernel / cpu / mcheck / mce_amd_64 . c
ipv4 : netfilter : ip_tables : fix information leak to userland
USB : EHCI : add check for wakeup / suspend race
ath9k / htc_drv_main : off by one error
i386 : fix NMI watchdog not reserving its MSRs
Btrfs : fix possible memory leak in btrfs_create_tree ()
mei : mei_irq_thread_write_handler check for overflow
fuse : Initialize total_len in fuse_retrieve ()
sunrpc : fix rpc debugging
mac80211 : Ensure initializing private mc_list in prepare_multicast ().
ipv6 mcast : Check address family of gf_group in getsockopt ( MS_FILTER ).
sound : snd_ctl_elem_add : fix value count check
[ GFS2 ] Fix calculation of demote state
drivers / w1 / w1_int . c : multiple masters used same init_name
drm / i915 : check for supported depth at fb init time
mm : numa : Do not account for a hinting fault if we raced
perf tools : callchain : Fix spurious ' perf report ' warnings : ignore empty callchains
drm / radeon : unlock the ring mutex while waiting for the next fence
ixgbe : Deny MACVLAN requests from VFs with admin set MAC
mlx4_core : Fix memory leak in mlx4_enable_msi_x ()
x86 / entry : Fix _TIF_USER_RETURN_NOTIFY check in prepare_exit_to_usermode
ALSA : hda - Fix digital mic on dell - m4 - 1 and dell - m4 - 3
[ media ] media : am437x - vpfe : return error in case memory allocation failure
Btrfs : stop leaking btrfs_bios on readahead
ASoC : Check for NULL register bank in snd_soc_get_cache_val ()
btrfs : fix null pointer dereference in btrfs_show_devname when name is null
regmap : Don ' t try to map non - existant IRQs
drbd : Fix module refcount leak in drbd_accept ()
ipv6 : Assume the loopback address in link - local scope .
Input : amijoy - add missing platform check
perf ui / tui : Print helpline message as is
[ PATCH ] e1000 : narrow down the scope of the tipg timer tweak
pvpanic : Set high notifier priority
staging : lustre : lustre : obdclass : obd_mount . c : Fix NULL dereference
cfq - iosched : fix no - idle preemption logic
drm / i915 : take struct_mutex in i915_dma_cleanup ()
powerpc / pseries : Fix possible leaked device node reference
ARM : LPC32xx : serial . c : Fixed loop limit
rtlwifi : rtl8192cu : Fix unitialized struct
userns : Fix posix_acl_file_xattr_userns gid conversion
ch7006 : correctly handling failed allocation
staging : rtl8188eu : Use ODM_REG_IGI_A_11N and ODM_BIT_IGI_11N in odm_DIGInit ()
USB : musb_gadget_ep0 : fix unhandled endpoint 0 IRQs , again
Input : usbtouchscreen - add support for LG Flatron T1710B
[ NETFILTER ]: nf_conntrack_sip : clear address in parse_addr ()
uvesafb : bitwise OR has higher precedence than ?:
mfd : vexpress - sysreg : Initialize ' site ' variable
batman - adv : Remove space before semicolon
ocfs2 : fix warning in ocfs2_file_aio_write ()
amd - xgbe : Check for complete packet on skb allocation error
qlcnic : Fix potential NULL dereference
[ media ] r820t : Fix IF scale
tcp / dccp : fix potential NULL deref in __inet_inherit_port ()
PCI : Fix oops in pci_vpd_truncate
cfg80211 : fix elapsed_jiffies calculation
ARM : imx : initialize clk_init_data . flags for clk - fixup - mux
NFSv4 : Return delegations synchronously in evict_inode
uas : Clear cmdinfo on command queue - ing
[ POWERPC ] allocation fix in ppc / platforms / 4xx / luan . c
[ media ] usbtv : correctly handling failed allocation
drm / nouveau : fix memory leak
[ PATCH ] tpm : fix memory leak
drm / ast : Use lockless gem BO free callback
drm / fb - helper : Do the ' max_conn_count ' zero check
powerpc / pseries : Fix of_node_put () underflow during DLPAR remove
tty / serial : at91 : fix race condition in atmel_serial_remove
[ PATCH ] audit : Moved variable declaration to beginning of function
greybus : power_supply : do not release failed supply alloc
bnx2x : properly handle CFC DEL in cnic flow
ocfs2 : fix check for locally granted state during dlmunlock ()
Input : ati_remote - add missing input_sync ()
[ media ] vb2 : Check if there are buffers before streamon
gen_nand : Test if nr_chips field is valid
isp1760 - hcd : use USB_DT_HUB
[ PATCH ] Clear task_struct -> fs_excl on fork ()
mm : highmem : export kmap_to_page for modules
staging : rtl8192e : don ' t just print an error and continue
ASoC : fix uninitialised variable in siu_dai . c
dm : bump the ioctl version to 4 . 32 . 0
fbcon : Fix bit_putcs () call to kmalloc ( s , GFP_KERNEL )
crypto : qat - Prevent dma mapping zero length assoc data
bonding : use l4 hash if available
drm / msm : fix deadlock in bo create fail path
[ NETFILTER ]: nfnetlink_queue : fix computation of allocated size for netlink skb .
powercap / RAPL : add IDs for future Xeon CPUs
i2c / i2c - omap : add a const qualifier
usb : musb : core : fix IRQ check
[ media ] cx88 - dsp : Use kmalloc_array () in read_rds_samples ()
Input : synaptics - do not retrieve the board id on old firmwares
perf intel - pt : Fix off - by - one comparison on maximum code
ext4 : add check for inodes_count overflow in new resize ioctl
KVM : ppc : deliver INTERRUPT_FP_UNAVAIL to the guest
[ ALSA ] Don ' t reject O_RDWR at opening PCM OSS with read / write - only device
pNFS / flexfiles : Fix a statistics gathering imbalance
btrfs : Fix the bug that fs_info -> pending_changes is never cleared .
stm class : Prevent user - controllable allocations
ath9k : Fix a crash in 2 WIRE btcoex chipsets
hfsplus : fix Buffer overflow with a corrupted image
dm : swap target postsuspend call and setting suspended flag
i2c / scx200_acb : Don ' t use 0 as NULL pointer
ALSA : line6 : fix ifnullfree . cocci warnings
wl12xx : protect wl12xx_op_set_rts_threshold ()
drm : shut up invalid edid messages
[ SCSI ] qla2xxx : Pad IOCB structure for size requirements .
tick / nohz : Fix possible missing clock reprog after tick soft restart
[ media ] s5p - fimc : Use pix_mp for the color format lookup
drm / i915 : Correctly reject invalid flags for wait_ioctl
bridge : vlan : fix usage of vlan 0 and 4095 again
[ media ] coda : add compressed flag to format enumeration output
KVM : fail KVM_SET_VCPU_EVENTS with invalid exception number
tcp : fastopen : fix rcv_wup initialization for TFO server on SYN / data
tulip : Properly check dma mapping result
Btrfs : return - 1 when lzo compression makes data bigger
[ ETH ]: Validate address in eth_mac_addr
acer - wmi : Add support for Aspire 1830 wlan hotkey
soc : ti : qmss : fix the case when ! SMP
Btrfs : fix em leak in find_first_block_group
ASoC : Intel : Skylake : Move up pipe mem free
ARM : 6135 / 1 : mx21 / devices : fix USBOTG resource
mm : numa : migrate : Set last_nid on newly allocated page
ipv4 : Fix fib_trie . c build , missing linux / vmalloc . h include .
ipv4 : Use FIELD_SIZEOF () in inet_init ().
staging / rtl8192u - check for skb alloc failure
handle recursive calls to bust_spinlocks ()
Input : ads7846 - remove redundant regulator_disable call
USB : gadgetfs : Fix a potential memory leak in ' dev_config ()'
tracing / function - graph - tracer : fix merge
TPM : tpm_nsc : Fix a double free of pdev in cleanup_nsc
cifs : Use BUILD_BUG_ON to validate uids and gids are the same size
[ ARM ] 5072 / 1 : corgi_pm : don ' t register devices on non - corgi machines
ocfs2_dlm : Missing get / put lockres in dlm_run_purge_lockres
libertas : down_interruptible () can return - EINTR , not EINTR
gma500 : reversed test in mdfld_dbi_dsr_exit ()
x86 , kaslr : Use char array to gain sizeof sanity
eCryptfs : Flush dirty pages in setattr
drm / i915 : try not to lose backlight CBLV precision
rt2x00 : rt2800lib : fix default TX power check for RT55xx
rsi : avoid format string leak to thread name
floppy : Add an extra bound check on ioctl arguments
mmap : check -> vm_ops before dereferencing
ehci - hcd : Fix typo in an error message
KVM : arm64 : vgic - its : Fix vgic_its_check_device_id BE handling
NFC : Clear pn533 target structure
HID : cp2112 : fix gpio - callback error handling
pinctrl : uniphier : fix . pin_dbg_show () callback
mmc : mmci : Cascade EPROBE_DEFER from regulators .
tools lib traceevent : Handle % z in bprint format
selinux : fix off - by - one in setprocattr
ath10k : enable Adaptive Noise Immunity ( ANI ) by default
staging : ozwpan : Return error , if PD is not connected .
[ media ] gspca / stv06xx_pb0100 : Don ' t potentially deref NULL in pb0100_start ()
ACPICA : debugger : fix memory leak on Pathname
cfg80211 : don ' t set privacy w / o key
x86 : trim ACPI sleep stack buffer
drm / sti : in crtc_atomic_flush , enable only planes of this crtc
drm / radeon / kms : prefer high post dividers in legacy pll algo
[ media ] mceusb : plug memory leak on data transmit
drm / i915 : fix integer overflow in i915_gem_execbuffer2 ()
ARM : S3C6410 : update clk -> parent when setting clock source
drm / i915 : Only check for valid PP_ { ON , OFF } _DELAYS on pre ILK hardware
b43 : stop format string leaking into error msgs
DMAENGINE : at_hdmac : locking fixlet
i2c : i2c - designware - platdrv : Drop class based scanning to improve bootup time
[ ARM ] 5315 / 1 : Fix section mismatch warning ( sa1111 )
iommu / vt - d : Fix NULL pointer dereference in device_to_iommu
drivers / net / wan : remove exceptional & on function name
telephony : fix return value
bsg : fix bsg_poll () to return POLLOUT properly
drm / amdgpu / gfx9 : bypass clockgating setting
ARM : S3C64XX : Staticise s3c6400_sysclass
sky2 : Shut off interrupts before NAPI
usb : renesas_usbhs : fixup : avoid NULL access on error case pipe detach
sfc : mark state UNINIT after unregister
AFS : Fix possible null pointer dereference in afs_alloc_server ()
drm / radeon / kms : although LVDS might be possible on crtc 1 don ' t do it .
mac80211 : allow channel change while mesh is down
greybus : call put_device () on error
drm / i915 : don ' t reallocate the compressed FB at every frame
[ XFRM ]: xfrm_migrate () needs exporting to modules .
lib / mpi / mpicoder . c : looping issue , need stop when equal to zero , found by ' EXTRA_FLAGS =- W '.
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
PCI : Prevent out of bounds access in numa_node override
x86 : e820 memmap - add checking for NULL early param
gpio / samsung : only register available gpio banks
[ media ] radio - maxiradio : fix memory leak when device is removed
ALSA : timer : fix division by zero after SNDRV_TIMER_IOCTL_CONTINUE
[ JFFS2 ] Add declaration of jffs2_lzo_ { init , exit } to compr . h
cw1200 : Fix cw1200_debug_link_id
KVM : cleanup "* new . rmap " type
clk : rockchip : rename rga clock - id on rk3399
[ ARM ] pxa : fix pxa27x_udc default pullup GPIO
[ PATCH ] hwrng : fix intel probe error unwind
[ IPV4 ] fib_trie : fix duplicated route issue
drbd : fix memory leak in drbd_adm_resize
Staging : rtl8821ae : Drop memory allocation cast for kmalloc in hal_btc . c
tipc : Add in missing lock during link initialization
drm / i915 : Kill leftover GTIIR writes from valleyview_irq_preinstall ()
[ media ] au8522_common : add missing MODULE_LICENSE
sfc : SFT9001 : Fix condition for LNPGA power - off
ath9k : fix 5 / 10MHz channel width initialization on ar9003
pm2fb : free cmap memory on module remove
x86 , ioapic : Throw BUG instead of NULL dereference
powerpc / pmac : Fix SMP kernels on pre - core99 UP machines
xfs : fix might_sleep () warning when initialising per - ag tree
[ media ] saa7134 : v4l2 - compliance : clear reserved part of VBI structure
perf probe : Trivial typo fix for -- demangle
Staging : rtl8192e : Use ! x instead of x == NULL
ALSA : hda - fix read before array start
mtd : atmel_nand : trivial : change DMA usage information trace
x86 - 32 : Fix sparse warning for the __PHYSICAL_MASK calculation
[ SCSI ] ibmvscsi : Correct DMA mapping leak
fb_defio : Push file_update_time () into fb_deferred_io_mkwrite ()
timers , drivers / tty / metag_da : Initialize the poll timer as pinned
sata - fsl : Apply link speed limits
KVM : SVM : Set the ' busy ' flag of the TR selector
[ media ] V4L : mt9t112 : use after free in mt9t112_probe ()
ath5k : initialize default noise floor
kernfs : update kernfs_rename_ns () to consider KERNFS_STATIC_NAME
mtd : mtk - nor : adjust sequence of trigger function and assignment function
hvc_xen : NULL dereference on allocation failure
xen / privcmd : print SIGBUS faults
Input : serio_raw - link serio_raw misc device to corresponding
ixgbe : add missing rtnl_lock in PM resume path
ALSA : usb : caiaq : check for cdev -> n_streams > 1
wext : verify buffer size for SIOCSIWENCODEEXT
greybus : greybus_protocol : fix order of sdio get caps response
usb : xhci : Fix OOPS in xhci error handling code
spi : add spi_message_init_no_memset to avoid zeroing the spi_message
xfrm_user : verify policy direction at XFRM_MSG_POLEXPIRE handler
ALSA : usb - audio : Fix quirks code is not called
gpio : GPIO_GET_CHIPINFO_IOCTL : Fix information leak
usb : gadget : storage : release superspeed descriptors .
amd64_edac : Fix potential memleak
[ media ] lgdt3306a : one bit fields should be unsigned
pinctrl : single : fix error return code in pcs_parse_one_pinctrl_entry ()
[ media ] media : vb2 - memops : Export vb2_get_vma symbol
ASoC : Intel : add support for Realtek 5651 on Cherrytrail
HID : egalax : Use kzalloc
mac80211 : fix potentially redundant skb data copying
bcma : support chipsets with PMU and GCI cores ( devices )
KVM : x86 : race between KVM_SET_GSI_ROUTING and KVM_CREATE_IRQCHIP
ipc / shm . c : check for integer overflow during shmget .
[ PATCH ] um : fix - up schedule_timeout () usage
[ SCSI ] Fix sense key MEDIUM ERROR processing and retry
staging : lustre : Replace max () with max_t ()
ARM : S3C64XX : DMA : Free node for non - circular queues
mac80211 : call dev_alloc_name before copying name to sdata
mac80211 : compete scan to cfg80211 if deferred scan fail to start
metag : export clear_page and copy_page
s390 / io : add ioport_map stubs
usb : isp1760 : check for null return from kzalloc
libceph : avoid NULL kref_put from NULL alloc_msg return
[ PATCH ] ieee1394 : fix cross_bound check for null ISO packets
viafb : fix IGA1 modesetting on VX900
pcmcia : fix incorrect bracketing on a test
crypto : md5 - Set statesize
staging : android : ion : ion_dummy_driver . c Replace kzalloc () by kcalloc ()
[ SPARC64 ]: Fix pfn_pte () build failure .
xenbus : avoid zero returns from read ()
pppoe : Lacks DST MAC address check
dmaengine : pl330 : Check if the DMA descriptor is NULL
um : don ' t leak floating point state and segment registers on execve ()
iwlwifi : mvm : don ' t clear tbl -> win mistakenly
sfc : Clear I2C adapter structure in falcon_remove_nic ()
[ media ] davinci : vpif_display : fix return type check for v4l2_subdev_call ()
irda : Correctly clean up self -> ias_obj on irda_bind () failure .
mips : handle pgtable_page_ctor () fail
Btrfs : fix clone ioctl where range is adjacent to extent
ipc / shm . c : check for ulong overflows in shmat
KVM : x86 : preserve x2APIC LDR on INIT
drivers / rtc / rtc - da9055 . c : remove use of regmap_irq_get_virq ()
drm / nouveau / pci / gp104 : initial support
cpufreq : prevent lockup on reading scaling_available_frequencies
staging : dgrp : fix potential NULL defereference issue
cpu / hotplug : Handle early registration gracefully
reparent_leader : check EXIT_DEAD instead of task_detached ()
futex : Prevent attaching to kernel threads
USB : Checking the wrong variable in usb_disable_lpm ()
drm / tilcdc : Remove unnecessary braces
x86 : Use string instruction memcpy on AMD Fam11h
cpufreq : powernow - k6 : Fix incorrect comparison with max_multipler
drm / radeon / kms : mmio base / size should be resource_size_t .
Staging : rc2860 : return - EFAULT on copy_to_user errors
dm mpath : fix NULL pointer dereference when path parameters missing
staging : line6 : Fix unlocked snd_pcm_stop () call
ASoC : fsl_ssi : Fix samples being dropped at Playback startup
ARM : 7010 / 1 : mm : fix invalid loop for poison_init_mem
[ media ] drxk_hard : fix bad alignments
drbd : merge_bvec_fn : properly remap bvm -> bi_bdev
video : ARM CLCD : Add missing error check for devm_kzalloc
crypto : qat - update init_esram for C3xxx dev type
skge : Unmask interrupts in case of spurious interrupts
ARM : mx3 : mx31lilly : pass mc13783 SPI IRQ pin
Input : twl4030 - pwrbutton - fix a leak of the IRQ during init failure
pinctrl : tegra - xusb : fix an off by one test
hwmon : ( w83627ehf ) Force initial bank selection
Btrfs : don ' t wait around for new log writers on an SSD
x86 , efi : Check table header length in efi_bgrt_init ()
perf top : Fix zero or negative refresh delay
iommu / vt - d : Remove to_pci_dev () in intel_map_page ()
drm / i915 : Shut down PCH interrupts during irq_uninstall
staging : comedi : adv_pci1710 : update the MODULE_DESCRIPTION
[ PATCH ] Signedness issue in drivers / net / phy / phy_device . c
isdn : mISDN : set -> family in -> getname ()
drivers / rtc / rtc - rv3029c2 . c : fix disabling AIE irq
Drivers : hv : balloon : Make the balloon driver not unloadable
mac80211 : extend fast - xmit to driver fragmentation
drm / nouveau : properly handle allocation failure in nouveau_sgdma_populate
cfg80211 : fix truncated IEs
[ SCSI ] lpfc : fix oops when parsing dodgy VPD
imx - drm : ipuv3 - plane : fix ipu_plane_dpms ()
xfrm_user : fix info leak in copy_to_user_policy ()
mei : abort waiting for notification on unsupported HW
x86 , mce : fix ifdef for 64bit thermal apic vector clear on shutdown
[ media ] r820t : better report signal strength
usb : wusbcore : read actual_length bytes isoc in segments
[ GFS2 ] kernel panic mounting volume
fs : befs : remove unnecessary * befs_sb variable
rtlwifi : rtl8192se : Fix gcc 4 . 7 . x warning
[ SCSI ] libfc : handle RRQ exch timeout
iommu / amd : Free domain id when free a domain of struct dma_ops_domain
UIO : uio_pdrv : fix memory leak
scsi : ibmvscsis : Properly deregister target sessions
KVM : PPC : e500 : Use kmalloc_array () in kvm_vcpu_ioctl_config_tlb ()
ocfs2 : goto out_unlock if ocfs2_get_clusters_nocache () failed in ocfs2_fiemap ()
block : remove padding from io_context on 64bit builds
video : s3c - fb : return an error when bpp is invalid
lm8333 : Fix check ordering
[ SPARC64 ]: Fix of_device bus_id settings .
usb : gadget : r8a66597 - udc : fix error return code in r8a66597_probe ()
spi : qup : Fix return value checking for pm_runtime_get_sync ()
ALSA : hda_intel : Add the Linux device ID for NVIDIA HDA controller
f2fs : avoid to trigger writepage during POR
dm bufio : initialize read - only module parameters
fuse : fix bad return value in fuse_file_poll ()
[ media ] cx22700 : Fix potential buffer overflow
ceph : properly set issue_seq for cap release
KVM : PIT : free irq source id in handling error path
[ PATCH ] v9fs : replace strlen on newly allocated by __getname buffers to PATH_MAX
UBIFS : check return code
spi : pxa2xx : missing break in pxa2xx_ssp_get_clk_div ()
net : make ipv6 PKTINFO honour freebind
[ PATCH ] Fix listxattr () for generic security attributes
rpc : fix garbage in printk in svc_tcp_accept ()
[ NETFILTER ]: nfnetlink_log : fix possible NULL pointer dereference
intel - iommu : fix off - by - one in pagetable freeing
hwmon : ( max31790 ) potential ERR_PTR dereference
staging : logger : hold mutex while removing reader
ALSA : snd - usb - caiaq : Lock on stream start / unpause
ocfs2 : Filter - ENOSPC in mlog_errno ()
p54 : eliminate warning for uninitialized variable ' tim_len '
backlight : Fix backlight limiting on spitz / corgi devices
iwlwifi : fix TX tracer
ath6kl : add missing of_node_put
drm / i915 / skl : Restore the DDI translation tables when enabling PW1
powerpc / eeh : Skip finding bus until after failure reporting
ARM : mx51 : Fix the address space length for SSI
b43 : Fix warning at drivers / mmc / core / core . c : 237 in mmc_wait_for_cmd
drm / amdgpu : update pd while updating vm as well
iommu : smar : Fix return value check of create_irq ()
Drivers : hv : vmbus : Fix a bug in vmbus_open ()
[ 9P ]: Fix missing unlock before return in p9_mux_poll_start
perf top : Add top . children config option
usb : gadget : net2280 : Fix NULL pointer dereference
ixgbe : balance free_irq calls with request_irq calls
misc : apds9802als : Fix the logic checking timeout in als_wait_for_data_ready ()
blk - mq : stop hardware queue in blk_mq_delay_queue ()
[ media ] cxd2820r : wait demod lock for DVB - C too
i40e : Expose ' trust ' flag to userspace via ndo_get_vf_config .
crypto : ahash - ensure statesize is non - zero
virtio : console : Make read () return - ENODEV on hot - unplug
mac80211 : simplify AP interface stop
NFSD : Put export if prepare_creds () fail
[ PATCH ] drivers / message / i2o / pci . c : fix a NULL pointer dereference
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
dma : pl330 : Set residue_granularity
inet : tcp : fix inetpeer_set_addr_v4 ()
drm / rockchip : return ERR_PTR instead of NULL
drivers / net / usb / usbnet . c : Use FIELD_SIZEOF macro in usbnet_init () function .
drm / i915 : take mode config lock around crtc disable at suspend
HID : i2c - hid : also call i2c_hid_free_buffers in i2c_hid_remove
staging : lustre : lustre : mdc : lproc_mdc . c : Fix for potential NULL pointer dereference
[ libata ] ata_piix : add HP compaq laptop to short cable list
fm10k : don ' t try to stop queues if we ' ve lost hw_addr
drivers / platform / x86 / amilo - rfkill . c :: amilo_rfkill_probe () avoid NULL deref
mmc : core : fix error path in mmc_host_alloc
cciss : add BUILD_BUG_ON () for catching bad CommandList_struct alignment
xfs : publish UUID in struct super_block
KVM : fix kvmclock regression due to missing clock update
perf probe : Fix regression of variable finder
sh : fix a kfree address in clkdev code
crypto : arm - replace memset by memzero_explicit
[ media ] v4l : of : Drop acquired reference to node when getting next endpoint
loop : fix NULL dereference if mount fails
tmiofb : missing NULL pointer checks
[ PATCH ] x86_64 : Fix swiotlb dma_alloc_coherent fallback
slub : When allocating a new slab also prep the first object
eCryptfs : protect crypt_stat -> flags in ecryptfs_open ()
perf record : No build id option fails
Staging : Fix leak in drivers / staging / at76_usb . c
OMAPDSS : HDMI5 : disable interlace modes
drm / radeon : don ' t share PPLLs on DCE4 . 1
drm / i915 / vlv : disable PPGTT on early revs v3
xtensa : fix potential NULL - pointer dereference
[ PATCH ] pcmcia : permit single - character identifiers
rtnl : fix msg size calculation in if_nlmsg_size ()
clk : si5351 : Fix clkout rate computation .
[ PATCH ] uml : fix sizeof usage
dma : imx - sdma : Assign a default script number for ROM firmware cases
cfq - iosched : handle failure of cfq group allocation
[ PATCH ] x86 - 64 : unexport cpu_llc_id
arm64 : fix NULL dereference in have_cpu_die ()
drm / exynos : exynos_hdmi : Pass correct pointer to free_irq ()
staging : slicoss : use status in slic_ioctl
crypto : eseqiv - Fix IV generation for sync algorithms
drm / i915 : Enable GMBUS for post - gen2 chipsets
drm / i915 : take struct mutex around fb unref
ceph : drop extra open file reference in ceph_atomic_open ()
drm / radeon / kms : Fix oops after radeon_cs_parser_init () failure .
ixgbe : use kzalloc for allocating one thing
i2c : robotfuzz - osif : Constify osif_table
latencytop : fix kernel panic while reading latency proc file
skge : Adjust a null pointer check in skge_down ()
[ PATCH ] m68knommu : switch 68360 to using rtc_time
vfio / spapr_tce : Check kzalloc () return when preregistering memory
[ ALSA ] emu10k1 - Fix STAC9758 front channel
tcp : fastopen : avoid negative sk_forward_alloc
Btrfs : Lower the max inline size to 8k
fbcon : delay the start of the cursor timer until a con_switch ()
perf symbols : Fix missing GNU IFUNC symbols
rrunner . c : fix possible memory leak in rr_init_one ()
mm : vmscan : fix misused nr_reclaimed in shrink_mem_cgroup_zone ()
gpio : aspeed : Add open - source and open - drain support
gpio : davinci : Fix possible NULL pointer deference
jbd2 : Fix unreclaimed pages after truncate in data = journal mode
isdn : avoid copying too long drvid
bcache : fix use - after - free in btree_gc_coalesce ()
[ ALSA ] sound / isa / ad1816a / ad1816a . c : check kmalloc () return value
[ SCSI ] aha152x : Fix oops on module removal
[ NETFILTER ]: H . 323 helper : fix possible NULL - ptr dereference
ARM : 7857 / 1 : dma : imx - sdma : setup dma mask
amd64_edac : fix ECC checking
iommu / ipmmu - vmsa : Restrict IOMMU Domain Geometry to 32 - bit address space
proc : Point / proc / net at / proc / thread - self / net instead of / proc / self / net
drm / i915 : Allow full PPGTT with param override
staging : fix go7007 - usb license
[ S390 ] cio : dont forget to set last slot to NULL in ccw_uevent ().
amd64_edac : simple fix to allow reporting of CECC errors
staging : dgap : fix checkpatch warning on sscanf usage
sctp : fix auth_hmacs field ' s length of struct sctp_cookie
mtd : spi - nor : improve macronix_quad_enable ()
scsi : handle command allocation failure in scsi_reset_provider
extcon : trivial : kfree missed from remove path
OMAPDSS : hdmi5 : Fix bit field for IEC958_AES2_CON_SOURCE
crypto : sahara - fix typo " Decidated " -> " Dedicated "
USB : sierra : fix urb and memory leak in resume error path
Input : polldev - immediately poll device upon opening
r8152 : replace strncpy with strlcpy
perf evlist : Fix max mmap_pages
sh_eth : enable wake - on - lan for sh7734
Staging : iio : Remove line over 80 characters
x86 : watchdog - check for CPU is being supported
mlxsw : spectrum : Don ' t take multiple references on a FID
int340x_thermal / processor_thermal_device : return failure when
cpufreq : Return error if -> get () failed in cpufreq_update_policy ()
libertas : don ' t clear priv -> dnld_sent after sending sleep confirm
ceph : clear migrate seq when MDS restarts
xenbus : remove transaction holder from list before freeing
[ SCSI ] pm8001 : bit set pm8001_ha -> flags
gpio : mcp23s08 : request a shared interrupt
ALSA : hda - Sync EAPD with vmaster on AD1984A Thinkpads
tcp : fix tcp_cong_avoid_ai () credit accumulation bug with decreases in w
ACPI : thinkpad - acpi : do not arm fan watchdog if it would not work
clk : hisilicon : Use devm_kmalloc_array () in hisi_clk_alloc ()
cxl : remove redundant increment of hwirq
bzip2 / lzma : fix decompress_inflate . c vs multi - block - with - embedded - filename
ext4 : error proc entry creation when the fs / ext4 is not correctly created
flex_array : fix the panic when calling flex_array_alloc () without __GFP_ZERO
spi : spi - ti - qspi : reinit of completion variable
[ ARM ] pxa / csb726 : register ac97 controller
drm / radeon : don ' t setup audio on asics that don ' t support it
drm / radeon : fix a bug in the SA code
staging : lustre : dne : setdirstripe should fail if not supported
[ PATCH ] USB : don ' t allocate dma pools for PIO HCDs
xfs : fix assertion failure at xfs_setattr_nonsize
Staging : ozwpan : prevent bogus dereference
batman - adv : Count all non - success TX packets as dropped
staging : rts_pstor : Complete scanning_done variable
spi : fix spi_s3c24xx_gpio device handle lookup
blkio : Set must_dispatch only if we decided to not dispatch the request
[ NETFILTER ]: nf_log : add netfilter gcc printf format checking
xfs : fix duplicate message output
b43 : fix support for 14e4 : 4321 PCI dev with BCM4321 chipset
apm_power : check I . intval for zero value , we use it as the divisor
Staging : comedi : dt2801 . c : off by one issue
[ PATCH ] manage - jbd - its - own - slab fix
V4L : Storage class should be before const qualifier
scsi_dh : return SCSI_DH_NOTCONN in scsi_dh_activate ()
[ GFS2 ] Fix memory allocation in glock . c
sched : Try to deal with low capacity , fix update_sd_power_savings_stats ()
Input : gamecon - fix off by one range check
staging / rts_pstor : remove braces {} in sd . c ( sd_sdr_tuning )
[ MIPS ] time : set clock before clockevent_delta2ns () in GT641xx .
gro : Fix bogus gso_size on the first fraglist entry
drm : fix division - by - zero on dumb_create ()
igb : cap interrupts at 20K per queue when in itr mode 3
be2net : fix pmac_id [] allocation size
KVM : fix fault page leak
qlcnic : Fix ordering of stats in stats buffer .
fs / exofs / ore_raid . c : replace count * size kzalloc by kcalloc
staging : comedi : pcl726 : update MODULE_DESCRIPTION
mmc : sdhci - esdhc - imx : silence a false curly braces warning
be2net : Bug fix to avoid soft lockup in loopback test .
ext3 : destroy sbi mutexes in put_super
iwlwifi : mvm : initialise ADD_STA before sending it to the firmware
[ PATCH ] device - mapper snapshot : bio_list fix
ASoC : omap - mcbsp : fix snd_pcm_hw_rule_add arguments
ALSA : hda - check NULL pointer when creating SPDIF controls
drm / nouveau : Fix detection of DDC - based LVDS on DCB15 boards .
[ SPARC64 ]: Use alloc_pci_dev () in PCI bus probes .
ASoC : Make struct snd_soc_card ' s dapm_widgets and dapm_routes const
ceph : add missing locking to protect i_snap_realm_item during split
e1000e : increase minimum frame size allowed
usb : chipidea : udc : zero - length packet is only needed for TX
Input : atmel_mxt_ts - add bootloader addresses for new chips
xfs : invalidate allocbt blocks moved to the free list
ravb : Add missing free_irq () call to ravb_close ()
ASoC : Free card DAPM context on snd_soc_instantiate_card () error path
drm / i915 : Fix up some stray to_i915 ( dev ) after a recent merge
w1 : w1_process () is not freezable kthread
ARM : zynq : initialize slcr mapping earlier
[ NETFILTER ]: xt_pkttype : Add explicit check for IPv4
Btrfs : add fiemap ' s flag check
[ PATCH ] hdaps : use ENODEV
mei : reset client connection state on timeout
x86 , quirk : Fix SB600 revision check
drm / i915 : Track the primary plane correctly when reassigning planes
sched / balancing : Fix ' local -> avg_load > sds -> avg_load ' case in calculate_imbalance ()
dm stripe : fix potential for leak in stripe_ctr error path
drm / radeon / dpm : fix typo in setting uvd clock
ext4 : remove another test in ext4_alloc_file_blocks ()
[ PATCH ] mv643xx_eth : Update dev -> last_rx on packet receive
net : avoid NULL deref in inet_ctl_sock_destroy ()
tmpfs : truncate prealloc blocks past i_size
igb : Bump version to 3 . 0 . 6
kvm / s390 : also set guest pages back to stable on kexec / kdump
USB : isp1760 : Use an IS_ERR test rather than a NULL test
perf tools : Do not auto - remove Children column if -- fields given
drm / nv40 / mpeg : fix context handling
[ media ] marvell - cam : Don ' t signal multiple frame completions in scatter / gather mode
hwmon : ( f75375s ) Disable setting DC fan control mode for F75373
ux500 : use _cansleep GPIO functions
usb : musb : blackfin : fix typo in new bfin_musb_vbus_status func
ocfs2 : Improve rename locking
[ S390 ] hwsampler : allow cpu hotplug
[ Bluetooth ] Revert session reference counting fix
arm / arm64 : KVM : Don ' t allow creating VCPUs after vgic_initialized
UBI : Silence an unintialized variable warning
tty : an overflow of multiplication in drivers / tty / cyclades . c
[ media ] em28xx - i2c : rt_mutex_trylock () returns zero on failure
ARM : OMAP2 +: Fix dmtimer set source clock failure
drivers / net / cxgb3 / cxgb3_main . c : prevent reading uninitialized stack memory
macvtap : advance iov iterator when needed in macvtap_put_user ()
atl1c : return PCI_ERS_RESULT_DISCONNECT on permanent failure
s2io : Don ' t receive packets when the napi budget == 0
drm / tilcdc : Take CRTC lock when calling tilcdc_crtc_disable ()
ehea : Fix TSO and hw checksums with non - accelerated vlan packets .
[ X25 ]: Restore skb -> dev setting in x25_type_trans ().
[ PATCH ] PCI : fix memory leak in MMCONFIG error path
mfd : Change tunnelcreek watchdog name on the lpc_sch subdevices array
staging : comedi : jr3_pci : check PCI BAR 0 size
p54 : print unknown eeprom fields
uwb : Fix handling IEs with empty IE data in uwb_est_get_size ()
Staging : ramzswap : Set block size to PAGE_SIZE
drivers / rtc / rtc - snvs . c : remove redundant of_match_ptr
Staging : slicoss : changes memory allocation style in slicoss . c
dns_resolver : assure that dns_query () result is null - terminated
drm / i915 : CR clock gating is recommend to be set on PineView
misc : mic : Fix for double fetch security bug in VOP driver
evm : properly handle INTEGRITY_NOXATTRS EVM status
ALSA : hda - Intialize more codec fields in snd_hda_codec_reset ()
iwlagn : overwrite EEPROM chain setting for 6250 devices
PCI : hv : Use the correct buffer size in new_pcichild_device ()
clk / axs10x : Clear init field in driver probe
acer - wmi : set the touchpad toggle key code to KEY_TOUCHPAD_TOGGLE
perf symbols : Fix random fd closing with no libelf
[ CIFS ] add extra null attr check
isdn / hisax : fix compiler warning on hisax_pci_tbl
rt2800 : fix wrong TX power compensation
[ media ] bt8xx : fix memory leak
logfs : maintain the ordering of meta - inode destruction
drm / amdgpu : update rev id for oland
mwifiex : fix corner case association failure
[ POWERPC ] EEH : Work with device endpoint , always
[ SCSI ] megaraid : Use the proper type to hold the irq number .
[ media ] DVB , DiB9000 : Fix leak in dib9000_attach ()
[ media ] drx - j : Fix detection of no signal
libata : drop WARN from protocol error in ata_sff_qc_issue ()
USB : cyberjack : fix buggy integer overflow test
mfd : Constify WM8994 regulator_init_data
Btrfs : don ' t make a file partly checksummed through file clone
USB : make the autosuspend workqueue thread freezable
f2fs : allocate new segment aligned with sections
Staging : rtl8712 : fix potential null pointer dereference
tty / serial / sirf : fix error propagation in sirfsoc_uart_probe ()
leak in do_ubd_request
thermal : imx : Fix possible NULL dereference .
KVM : PPC : Book3S HV : Don ' t try to signal cpu - 1
arch / powerpc / sysdev / fsl_rmu . c : introduce missing kfree
ASoC : pcm3168a : Call clk_set_rate in pcm3168a_set_dai_sysclk
tpm / st33zp24 / spi : Improve st33zp24_spi_evaluate_latency
ALSA : asihpi : Fix unlocked snd_pcm_stop () call
[ PATCH ] relax ide - cd dma restrictions
exit : proc : don ' t try to flush / proc / tgid / task / tgid
gpio : use kzalloc to allocate gpio_device
ALSA : lx646es : Fix possible uninitialized variable reference
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
[ SCSI ] qla2xxx : Correct IOCB queueing mechanism for ISP54XX HBAs .
drm / i915 : enable HDMI mode on VLV when an HDMI sink is detected
cxgb4 : fix memory leak of qe on error exit path
crypto : talitos - avoid memleak in talitos_alg_alloc ()
[ ALSA ] gusextreme : set codec_flag
x86 / apic : Use x2apic physical mode based on FADT setting
drivers / staging / lustre : Fix set - but - unused whinge .
mac80211 : fix ieee80211_sta_process_chanswitch for 5 / 10 MHz channels
pcmcia : check for pointer instead of pointer address
staging : sep : add missing misc_deregister () on error in sep_register_driver_with_fs ()
mfd : palmas : Initialise client -> of_node for dummy created client
staging : unisys : remove S8 type
KVM : ARM : VGIC : Optimize the vGIC vgic_update_irq_pending function .
drivers / net / hamradio : Integer overflow in hdlcdrv_ioctl ()
[ ATM ] firestream : Fix uninitialized var warning .
inetpeer : fix token initialization
PCI : Use dev_is_pci () to identify PCI devices
[ media ] lirc_igorplugusb : Fix a compilation waring
tridentfb : fix hang on Blade3D with CONFIG_CC_OPTIMIZE_FOR_SIZE
perf tools : Change perf_guest default back to false
KVM : MMU : make for_each_shadow_entry aware of largepages
mfd : Check t7l66 platform_data pointer
dmaengine : omap - dma : Add more debug information when freeing channel
[ media ] tda8290 : Fix a bug if no tuner is detected
drm / i915 : Simplify i915_gem_obj_is_pinned () test for set - tiling
[ PATCH ] v9fs : fix fd_close
[ SCSI ] mpt2sas : add missing allocation .
drm / vmwgfx : Don ' t flush fb if we ' re in the suspended state .
[ ARM ] pxa : add missing irq events for pxa168
sysctl : require CAP_SYS_RAWIO to set mmap_min_addr
bonding : use rcu_access_pointer () in bonding_show_mii_status ()
drm / exynos : check crtc ' s dpms mode at SetCrtc
[ media ] dvb_demux : fix deadlock in dmx_section_feed_release_filter ()
ext4 : Fix ext4_free_blocks () w / o a journal when files have indirect blocks
drm / i915 : Convert wait_for ( I915_READ ( reg )) to intel_wait_for_register ()
[ PATCH ] inotify : fix idr_get_new_above usage
drm / i915 / dp : add lane_count check in intel_dp_check_link_status
ARM : mach - imx : clk - pllv2 : Staticize clk_pllv2_ops
tracing : Use TRACE_MAX_PRINT instead of constant
iwlwifi : fix PS disable status race
ASoC : rsnd : rsnd_dai_connect () returns error if it connect to existing mod
phy : Add reset callback for not generic phy
Btrfs : don ' t loop in allocator for space cache
cpufreq : brcmstb - avs - cpufreq : remove unnecessary platform_set_drvdata ()
ipv6 : Fix mem leak in rt6i_pcpu
fealnx : Write outside array bounds
mac802154 : llsec : add forgotten list_del_rcu in key removal
f2fs : fix wrong max cost initialization
USB : missing error check in emi26
svcrdma : Free context on post_recv error in send_reply
trusted - keys : check for NULL before using it
ASoC : intel : log an error on double free
selinux : check ss_initialized before revalidating an inode label
[ PATCH ] hugetlb : check p ? d_present in huge_pte_offset ()
firewire : nosy : fix IRQ handler for card ejection
staging : bcm2835 - audio : Fixed initialization of global to " false "
unix / dgram : peek beyond 0 - sized skbs
greybus : uart fix missing negation on DTR setting
mlx5_core : Fix out arg size in access_register command
[ PATCH ] VFS : memory leak in do_kern_mount ()
drm / radeon : Fail fb creation from imported dma - bufs .
ipv6 - multicast : Fix memory leak in input path .
[ PATCH ] imsttfb : Fix resource leak
drm / nouveau / i2c / gp104 : initial support
perf hists browser : Add NULL pointer check to prevent crash
[ PATCH ] m32r : __cmpxchg_u32 fix
ARM : SMP : remove IRQ - disabling for smp_cross_call ()
perf bench : Fix NULL pointer dereference in " perf bench all "
drm / amdgpu / si : load the proper firmware on 0x87 oland boards
igb : add i211 to i210 PHY workaround
b43 : Force - wake queues on init
[ SCSI ] libfcoe : don ' t send ELS in FIP mode if no FCF selected
[ PATCH ] Uninitialized variable in drivers / net / wan / syncppp . c
qxl : check for kmap failures
[ IPV4 ]: Make tcp_input_metrics () get minimum RTO via tcp_rto_min ()
[ PATCH ] USB : get USB suspend to work again
wl12xx : fix a memory leak of probereq template upon recovery
drm : i915 : Explicitly apply PWM config extracted from pwm_args
spi : sh - msiof : Fix warnings due to improper casts
tmpfs : fix highmem swapoff crash regression
ASoC : Return correct codec clock in s3c64xx - i2s
ath9k : deny new interface addtion on IBSS mode
perf session : Fix crash with invalid CPU list
x86 : Re - get cfg_new in case reuse / move irq_desc
selinux : fix off - by - one in setprocattr
NFS : Return directly if encode_sessionid fail
fs / ramfs / file - nommu . c : replace count * size kzalloc by kcalloc
kernel / kmod . c : check for NULL in call_usermodehelper_exec ()
btrfs : reada : move reada_extent_put to place after __readahead_hook ()
staging : wilc1000 : remove spaces around '->'
spi - imx : initialize complete config struct
TG3 : limit reaches - 1
sdhci : make isr tolerant of read errors
mtd : blkdevs : fix crash related to sysfs attributes .
dlm : close othercons
tracing : fix ref count in splice pages
misc : mei : set IRQF_ONESHOT for msi request_threaded_irq
slab : convert last use of __FUNCTION__ to __func__
edac : mv64x60 fix get_property
x86 / mce / AMD : Make the init code more robust
usb : dwc3 : gadget : return error if command sent to DGCMD register fails
Input : rohm_bu21023 - fix handling of retrying firmware update
bpf , mips : fix off - by - one in ctx offset allocation
macvlan : make operstate and carrier more accurate
[ ALSA ] ca0106 - Add missing sysfs device assignment
rbd : reduce memory used for rbd_dev fields
[ PATCH ] Fix warning with b44 . c on 64bit boxes
p54usb : fix leaks at failure path in p54u_probe ()
drivers / w1 / masters / omap_hdq . c : fix missing mutex unlock
f2fs : initialize newly allocated dnode structure
f2fs : fix wrong condition for unlikely
ALSA : snd - powermac : timeout reaches - 1
af_packet : remove unnecessary BUG_ON () in tpacket_destruct_skb
ARM : EXYNOS : fix GIC using for EXYNOS5440
tracing / filters : free filter_string in destroy_preds ()
i2c : i2c - bcm - kona : Fix module build
drm / amd / amdgpu / amdgpu_ttm : Handle return NULL error from ioremap_nocache
libceph : avoid NULL kref_put when osd reset races with alloc_msg
dmaengine : coh901318 : Only calculate residue if txstate exists .
dm snapshot : fix metadata corruption
ssb : Use pci_is_pcie ()
vmwgfx : information leak in vmw_execbuf_copy_fence_user ()
xfrm : Ignore socket policies when rebuilding hash tables
drm / radeon / kms / DCE4 . 1 : ss is not supported on the internal pplls
[ PPPOE ]: Missing result check in __pppoe_xmit ().
KVM : Grab pic lock in kvm_pic_clear_isr_ack
drm / radeon / atom : workaround vbios bug in transmitter table on rs780
davinci : DM365 : Update NAND driver platform data
arch / x86 / kernel / kdebugfs . c : introduce missing kfree
lib : list_sort_test (): add extra corruption check
NetLabel : the CIPSOv4 passthrough mapping does not pass categories correctly
x86 / boot : Fix minor fd leakage in tools / relocs . c
[ PATCH ] USB : trivial DEBUG message correction in gadget ether driver
sound : oxygen : zero - initialize model data
[ media ] uvcvideo : Fix race of open and suspend in error case
bnx2x : set_one_mac_e1x uses raw ' s state as input
rxrpc : Fix oops on incoming call to serviceless endpoint
appletalk : fix atalk_getname () leak
ASoC : soc - core : Fix codec -> name memory leak
e1000e : Correctly include VLAN_HLEN when changing interface MTU
sch : add missing u64 in psched_ratecfg_precompute ()
Btrfs : remove btrfs_try_spin_lock
slab : fix typo in allocation failure handling
pinctrl : sunxi : Fix potential null pointer dereference
usb : early : fixed coding style issue related to : operator
mm , fs : check for fatal signals in do_generic_file_read ()
iio : adc : ad799x : remove space before comma
[ TCP ]: Fix oops caused by __tcp_put_md5sig_pool ()
Staging : rtl8192e : Fix potential NULL pointer dereference
usb : gadget : bdc_udc : set value for common is_selfpowered
clk : divider : Fix table round up function
ocfs2 : honor O_ ( D ) SYNC flag in fallocate
smc91c92_cs . c : fix bogus compiler warning
[ ARM ] 4056 / 1 : iop13xx : fix resource . end off - by - one in flash setup
x86 / apic : Fix the devicetree build in certain configs
usb : udc : allow adding and removing the same gadget device
[ PATCH ] TPM : fix failure path leak
drm : WARN when removing unallocated node
ceph : skip mds sync on forced unmount
remove header_ops bug in qeth driver
IPv6 : use ipv6_addr_copy () in ip6_route_redirect ()
mac80211 : fix rate control initialisation
staging : comedi : disallow COMEDI_DEVCONFIG on non - board minors
firewire : fw - cdev : reorder wakeup vs . spinlock
iio : ina2xx : give the capture kthread a more useful name string .
hwmon : ( emc1403 ) Remove unnecessary hwmon_device_unregister
[ PATCH ] x86 - 64 : add memcpy / memset prototypes
virtio_net : enable big packets for large MTU values
blkcg : fix double free of new_blkg in blkcg_init_queue
pinctrl : s3c24xx : off by one in s3c24xx_eint_init ()
samples / bpf : Reset global variables
md : fix bug in raid10 recovery .
iio : ti_am335x_adc : Fix : Use same step id at FIFOs both ends
cpuset : fix RCU lockdep splat in cpuset_print_task_mems_allowed ()
linux / kernel . h : define SYMBOL_PREFIX
[ ARM ] 5402 / 1 : fix a case of wrap - around in sanity_check_meminfo ()
mfd : Fix return type of lm533 attribute is_visible
slub : Discard slab page when node partial > minimum partial number
fs : make inode_to_bdi () handle NULL inode
slab : fix alien cache handling
block / partitions / aix . c : replace count * size kzalloc by kcalloc
iommu / exynos : Add / remove callbacks should fail if no iommu is available
cyclades : fix read buffer overflow
aoe : error printed 1 too early
[ PATCH ] Fix typo in x86_64 __build_write_lock_const assembly
dm stats : fix a leaked s -> histogram_boundaries array
mac80211 : fix txq queue related crashes
[ PATCH ] gss_spkm3 : fix error handling in module init
atm : deal with setting entry before mkip was called
ipc / shm . c : check for overflows of shm_tot
vmwgfx : return - EFAULT if copy_to_user fails
mm : vma_adjust : remove superfluous check for next not NULL
pty : Fix packet mode setting race
qed : Fix error return code in qed_resc_alloc ()
cpufreq : update user_policy .* on success
vhost : actually track log eventfd file
OMAP : DSS2 : Taal : Fix ESD check
[ TCP ] FRTO : Response should reset also snd_cwnd_cnt
staging : comedi : prevent auto - unconfig of manually configured devices
[ PATCH ] i386 : fix flat mode numa on a real numa system
dlm : fix possible use - after - free
KVM : fix cpuid E2BIG handling for extended request types
usb : gadget : ci13xxx_pci : guard against devices without driver_data
sfc : Use kzalloc () to ensure struct efx_spi_device is fully initialised
[ TCP ]: Fix TCP header misalignment
mac80211 : Don ' t buffer non - bufferable MMPDUs
hwmon : ( lm87 ) Fix vrm write operation
usb : dwc3 : ep0 : move to CONFIGURED also on delayed status
dlm : make posix locks interruptible
orangefs : avoid freeing a slot twice in wait_for_direct_io ()
drivers / usb / misc / auerswald : fix status check , remove redundant check
tty / vt : Fix the memory leak in visual_init
AUDIT : Fix user pointer deref thinko in sys_socketcall ().
ide : add missing rq . ref_count initialization to ide_diag_taskfile ()
staging : rtl8188eu : Use kcalloc instead of kzalloc
ath9k : enable ext channel pulse detection
i40e : Fix a port VLAN configuration bug
ARM : S3C64XX : Remove unused CPU devices for Cragganmore
ALSA : hdspm - Abort on unknown firmware revision
staging : ks7010 : change length type to unsigned
[ PATCH ] NPTL signal delivery deadlock fix
Fix a referral error Oops
x86 : fix MP_processor_info section mismatch warning
IPoIB : Clear ipoib_neigh . dgid in ipoib_neigh_alloc ()
futexes : Fix infinite loop in get_futex_key () on huge page
ASoC : atmel - pdmic : add missing clk_disable_unprepare () on error in atmel_pdmic_cpu_dai_startup ()
greybus : loopback : Send some sensible data
clk : max77686 : fix number of clocks setup for clk_hw based registration
staging : rts5208 : Unnecessary parentheses cleanup
drm / i915 / skl : Don ' t skip mst encoders in skl_ddi_pll_select ()
ath9k : Check virtual wiphy state on tx ()
Btrfs : adjust NULL test
iscsi - target ; Enforce 1024 byte maximum for CHAP_C key value
tty : fix vt_compat_ioctl
[ ARM ] Use kcalloc to allocate counter_config array rather than kmalloc
USB : belkin_sa : avoid divide by zero error
acpi - cpufreq : Do not load on K8
brcmfmac : check return from kzalloc in brcmf_fweh_process_event
perf tools : Try to lookup kernel module map before creating one
sfc : Really allow RX checksum offload to be disabled
r8152 : check RTL8152_UNPLUG for rtl8152_close
[ POWERPC ] spufs : fix another off - by - one bug in spufs_mbox_read
device - dax : check devm_nsio_enable () return value
be2net : Check for POST state in suspend - resume sequence
inetpeer : initialize -> redirect_genid in inet_getpeer ()
[ PATCH ] 2 . 6 . 17 missing a call to ieee80211softmac_capabilities from ieee80211softmac_assoc_req
ASoC : Samsung : Add I2S S / W RST in startup function
ARM : imx : ehci : use void __iomem * to hold i / o addresses
iwlwifi : mvm : BT coex - fix BT prio for probe requests
drm / i915 / skl : Ensure pipes with changed wms get added to the state
USB : g_mass_storage : min (...) warning fixed
[ SCSI ] ibmvfc : Fix invalid error response handling
Staging : sep : potential buffer overflow in ioctl
libata : add SATA PMP revision information for spec 1 . 2
spi_mpc8xxx : Fix uninitialized variable
KVM : PPC : Initialize linears with zeros
drbd : fix spurious protocol error
mwifiex : block data traffic to tx paused receive address
drm / radeon : add backlight quirk for hybrid mac
[ media ] cx23885 - dvb : check if dvb_attach () succeded
ceph : fix ack counter reset on connection reset
ALSA : hda - Clear left - over hp_pins in snd_hda_parse_pin_def_config ()
tty : xuartps : Fix " may be used uninitialized " build warning
ASoC : Do not write to invalid registers on the wm9712 .
[ IPV6 ] ROUTE : Allow searching subtree only .
pkt_sched : Add queue stopped test back to qdisc_run ().
w1 : fix for loop in w1_f29_remove_slave ()
Btrfs : disable snapshot aware defrag for now
iommu / vt - d : Check the return value of iommu_device_create ()
x86 / amd - iommu : Don ' t detach device from pt domain on driver unbind
inkern : iio_device_put after incorrect return / goto
i40evf : do not re - arm watchdog after remove
uas : Fix bounds check in uas_find_endpoints
ASoC : Intel : Skylake : Initialize NHLT table
drm / i915 : Prevent use - after - free of inherited framebuffer
[ SCSI ] advansys : Fix bug in AdvLoadMicrocode
perf bench : Fix memcpy benchmark for large sizes
[ S390 ] s390 / setup : set default preferred console device " ttyS "
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
sctp : kzalloc () error handling on deleting last address
ALSA : line6 : Give up on the lock while URBs are released .
drm / rockchip : Shutdown all crtcs when unbinding drm
mtd : mtd - abi : add a helper to detect the nand type
mac80211 : fix register_hw error path
qlcnic : Initialize dcbnl_ops before register_netdev
ALSA : asihpi : incorrect range check
cfg80211 : fix potential connection work crash
dma : timb_dma : Fix compiler warning
mmc : sd : warn if card stays busy during init
carl9170 : fix typo in PS code
drm / i915 : Skip stolen region initialisation if none is reserved
drm / nouveau / bios / gp104 : initial support
Staging : wlan - ng : wiphy_free () is not called in case wiphy_register () fails
drm / amdgpu : add zero timeout check in amdgpu_fence_wait_seq_timeout
ecryptfs : initialize new auth_tokens before teardown
ARM : 6891 / 1 : prevent heap corruption in OABI semtimedop
drm / amdgpu : bypass RLC init for SRIOV
pti : PTI semantics fix in pti_tty_cleanup .
clk : tegra : Register the proper number of resets
rbd : fix a memory leak in rbd_get_client ()
lsm : Use a compressed IPv6 string format in audit events
[ IA64 ] SN2 : security hole in sn2_ptc_proc_write
gpio : moxart : Use devm_gpiochip_add_data () for gpio registration
iommu / amd : Fix BUG when faulting a PROT_NONE VMA
staging : lustre : lnet : make sure lnet data not greater than LIBCFS_IOC_DATA_MAX
Staging : wilc1000 : Replace memset with eth_zero_addr
sysfs : Make poll behaviour consistent
[ media ] cx231xx : add a missing break statement
sched : terminate newidle balancing once at least one task has moved over
ASoC : Fix incorrect kfree in wm8731_probe error path
spi : davinci : do not use DMA for vmalloc ' ed buffers
[ media ] stb0899 : sign of CRL_FREQ doesn ' t depend on inversion
ASoC : ad1836 : fix a memory leak if another ad1836 is registered
[ MTD ] Initialise s_flags in get_sb_mtd_aux ()
mm : fail prefaulting if page table allocation fails
iwlagn : save the latest smps mode
crypto : caam - Fix first parameter to caam_init_rng
ALSA : timer : Fix leak in SNDRV_TIMER_IOCTL_PARAMS
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
fcoe : ensure that skb placed on the fip_recv_list are unshared
mfd : ab8500 - debugfs : Apply a check for - ENOMEM after allocating memory for sysfs
block : ioctl : fix information leak to userland
staging : unisys : visornic : remove driver version from visornic_main . c
Staging : bcm : info leak in ioctl
clk : hisilicon : Use kcalloc () in hisi_clk_init ()
ocfs2 : avoid a pointless delay in o2cb_cluster_check ()
cxgb4vf : Use defined Mailbox Timeout
cdc_ncm : return - ENOMEM if kzalloc fails
[ IA64 ] missing exports hwsw_sync_ ...
crypto : img - hash - Fix hash request context
dmaengine / ste_dma40 : fix introduced warnings
atm : ambassador : use NULL instead of 0 for pointer
drm / nouveau / bus / gp104 : initial support
ATM : mpc , fix use after free
pty : fix data loss when stopped (^ S /^ Q )
xfrm : Don ' t allow esn with disabled anti replay detection
f2fs : fix inconsistency of block count during recovery
xhci : Warn when hosts don ' t halt .
pinctrl : pinctrl_select_state : set the old_state back on error
[ PATCH ] Bluetooth : fix potential NULL ptr deref in dtl1_cs . c :: dtl1_hci_send_frame ()
tracing : instance_rmdir () leaks ftrace_event_file -> filter
gpiolib : Fix NULL pointer deference
[ GFS2 ] Plug an unlikely leak
rtl8150 : rtl8150_disconnect (...) does not need tasklet_disable (...)
cgroups : make unlock sequence in cgroup_get_sb consistent
[ PATCH ] v9fs : return the correct error when interrupted by signal
Staging : gs_fpgaboot : Replace ' int32_t ' with ' int '.
drm / i915 : make sparse happy for the new vlv mmio read function
Btrfs : don ' t leak reloc root nodes on error
caif - hsi : Add missing return in error path
xfs : extra semi - colon breaks a condition
mtd : nand : tmio_nand : show parent device in sysfs
sky2 : only enable Vaux if capable of wakeup
HID : logitech - hidpp : add boundary check for name retrieval
brcmfmac : rework pointer trickery in brcmf_proto_bcdc_query_dcmd ()
Input : regulator - haptic - fix module autoload for OF platform driver
ipr : Driver version 2 . 6 . 2
thinkpad_acpi : Fix a memory leak during module exit
[ AF_KEY ]: Fix bug in spdadd
[ PATCH ] powerpc : make OF interrupt tree parsing more strict
drm / i915 : Make aliasing a 2nd class VM
usb : otg : Make USB3319 ULPI ID generic
md : Fix raid1 -> raid0 takeover
ALSA : fm801 : explicitly free IRQ line
tracing : Don ' t make assumptions about length of string on task rename
i40e : add check for null VSI
mac80211 : avoid aggregation for VO traffic
fbdev : bf537 - lq035 : add missing blacklight properties type
tools lib traceevent : Free filter tokens in process_filter ()
blk - mq : export blk_mq_free_request ()
[ SCSI ] ibmvfc : Fix DMA mapping leak on memory allocation failure
i2c : omap : on -> remove () call pm_runtime_put_sync ()
staging : lustre : osc : osc_extent_wait () shouldn ' t be interruptible
drm / radeon : fix sad_count check for dce3
Btrfs : fix memory leak in btrfs_parse_early_options ()
UBI : return on error in rename_volumes ()
[ PATCH ] dm table split_args : handle no input
reiserfs : Don ' t call reiserfs_get_acl () with the reiserfs lock
staging : unisys : visorchannel : Place logical continuation at the end of a line
mtg : docg3 : potential divide by zero in doc_write_oob ()
tun : update file current position
f2fs : set dirty state for filesystem only when updating meta data
brcm80211 : Don ' t leak ' vbuffer ' in brcmf_sdbrcm_write_vars ()
spidernet : node - aware skbuff allocation
gpio : GPIOHANDLE_GET_LINE_VALUES_IOCTL : Fix information leak
vhost : copy_to_user -> __copy_to_user
arm64 : kernel : do not mark reserved memory regions as IORESOURCE_BUSY
[ PATCH ] Fix sys_migrate_pages : Move all pages when invoked from root
jme : Fix VLAN memory leak
r8152 : check tx agg list before spin lock
regmap : Silence warning on invalid zero length read
leds : add LED_ON brightness as boolean value
workqueue : Add quiescent state between work items
[ PATCH ] md : Avoid oops when attempting to fix read errors on raid10
btrfs : sysfs label interface should check for read only FS
spi : spi - fsl - dspi : Fix incorrect freeing of DMA allocated buffers
gro : Reset dev pointer on reuse
Btrfs : Fix test_range_bit for whole file extents
fsl_pq_mdio : Revive Gianfar TBI PHY support
tty : serial : Use GFP_ATOMIC instead of GFP_KERNEL in serial8250_em485_init ()
[ PATCH ] x86 - 64 : remove prototype of free_bootmem_generic ()
staging : usbip : Fix deadlock
brcmfmac : fix possible memory leak
gpu : host1x : Check for valid host1x pointer
drm / i915 : Fix NULL pointer deference when out of PLLs in IVB
drm : fixes when i2c encoder slave mode_fixup is null .
USB : idmouse : fix NULL - deref at probe
ocfs2 : unmap_mapping_range () in ocfs2_truncate ()
powerpc / crypto / crc32c - vpmsum : Fix missing preempt_disable ()
staging : rts5208 : Spacing
md : check for memory allocation failure in faulty personality
iwlwifi : mvm : Avoid signal based decisions if ave beacon RSSI is 0
[ SCSI ] qla4xxx : correct return status in function qla4xxx_fw_ready
sysfs : Remove double free sysfs_get_sb
staging : usbip : userspace : libsrc : ( foo *) should be ( foo *)
slub : Zero initial memory segment for kmem_cache and kmem_cache_node
quota : Fix WARN_ON in lookup_one_len
block : Enable sysfs nomerge control for I / O requests in the plug list
agp / intel : Use a non - reserved value for the cache field of the PTEs .
ARM : KVM : Bugfix : vgic_bytemap_get_reg per cpu regs
ibm_newemac : Fix dangerous struct assumption
tty_audit : fix checkpatch complaint
drm / exynos : fimd : fix broken dp_clock control
[ media ] xc2028 : add missing break to switch
iio : adc : mcp320x : Set struct spi_driver . of_match_table
ASoC : Intel : Fix incorrect sizeof () in sst_hsw_stream_get_volume ()
xen - blkfront : remove IRQF_SAMPLE_RANDOM which is now a no - op
arm : mxs : add irq_chip - name for GPIO IRQs
sched : Don ' t set sd -> child to NULL when it is already NULL
mfd : Fix possible s5m null pointer dereference
staging : zcache : fix " zcache =" kernel parameter
mtd : elm : Use correct check on return value of pm_runtime_get_sync
mwl8k : prevent freeing free IRQ if ieee80211_register_hw () fails
[ media ] s5p - g2d : Remove casting the return value which is a void pointer
ASoC : Reset WM8962 with default ID value
[ media ] xc5000 : use after free in release ()
i40evf : don ' t blow away MAC address
greybus : uart : Update line coding settings only when needed
igb : use mac loopback for i354 backplane
drm / i915 : Return stored value from max freq sysfs entry
staging : dwc2 : fix thinko in dwc2_hc_set_even_odd_frame ()
staging : iio : compress return logic
slob : fix memory corruption
Btrfs : fix bitmap size tracking
[ PATCH ] s390 : overflow in sched_clock
p54 : use ether_addr_equal_64bits
drivers / net / wireless / p54 / txrx . c Fix off by one error
drm : mxsfb : Fix crash when provided invalid DT bindings
[ PATCH ] invalidate_inode_pages2_range () debug
ASoC : omap - pcm : off - by - one in ' omap_pcm_limit_supported_formats '
x86 / microcode / intel : Guard against stack overflow in the loader
ACPI : check acpi_disabled in acpi_table_parse () and acpi_table_parse_entries ()
igb : fix stats for i210 rx_fifo_errors
HID : extend autodetect to handle I2C sensors as well
r8169 : correct phy parameters for the 8110SC
vxlan : Update hard_header_len based on lowerdev when instantiating VXLAN
HID : hid - magicmouse : Add pointer and buttonpad properties for Magic Trackpad
ASoC : nuc900 - ac97 : fix a memory leak
m68k : Export empty_zero_page for ZERO_PAGE usage in modules .
staging : ozwpan : Check error condition before creating endpoint .
spi : davinci : remove unnecessary completion variable initialization
perf probe : Fix the missed parameter initialization
do_wait () wakeup optimization : child_wait_callback : check __WNOTHREAD case
OMAPDSS : add missing export for omap_dss_get_output ()
bna : ethtool : Avoid reading past end of buffer
selinux : fix setting of security labels on NFS
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
ath9k / htc_drv_main : off by one error
Btrfs : fix possible memory leak in btrfs_create_tree ()
HID : fix error condition propagation in hid - sony driver
[ media ] Staging : media : platform : davinci : - Fix for memory leak
perf tools : Make vmlinux short name more like kallsyms short name
fuse : Initialize total_len in fuse_retrieve ()
powerpc / pci : export pcibios_free_controller ()
devm_memremap : Fix error value when memremap failed
ath9k_hw : fix an off - by - one error in the PDADC boundaries calculation
Staging : comedi : rtd520 : &&/|| typo
ALSA : hda / realtek - Avoid invalid COEFs for ALC271X
drm / vmwgfx : Clear an uninitialized struct member
[ media ] soc - camera : change order of removing device
[ PATCH ] s3c2410fb : fix un - initialised dev field
mwifiex : update amsdu tx packet time stamp
[ CPUFREQ ] Longhaul - Fix power state test to do something more useful
[ media ] lirc : cannot read from tx - only device
Btrfs : stop leaking btrfs_bios on readahead
staging : frontier : use after free in disconnect ()
usb : xhci - plat : properly handle probe deferral for devm_clk_get ()
[ PATCH ] md : bio leak fix
sis - agp : Remove SIS 760 , handled by amd64 - agp
x86 , PAT : Changing memtype to WC ensuring no WB alias
iwlwifi : fix PCI - E transport " inta " race
[ SPARC ]: Fix exec failures on sun4c .
drm / radeon : add a dpm quirk for all R7 370 parts
ASoC : tlv320dac33 : Avoid powering off while in BIAS_OFF
usb : gadget : u_ether : fix dereference after null check coverify warning
perf kvm record / report : ' unprocessable sample ' error while recording / reporting guest data
fix IN_DELETE_SELF on overwriting rename () on ramfs et . al .
cpuset : use trialcs -> mems_allowed as a temp variable
sparc64 : Fix overshoot in nid_range ().
staging : lustre : llite : fixup return value ll_direct_IO_26
ixgbe : Disallow device reset during ethtool test
cifs : potential missing check for posix_lock_file_wait
net / 9p / protocol . c : Fix a memory leak
mtd : socrates_nand : Remove redundant dev_set_drvdata
ch7006 : correctly handling failed allocation
batman - adv : randomize initial seqno to avoid collision
staging : tidspbridge : fix potential array out of bounds write
mlx4_core : Fix array overrun in dump_dev_cap_flags ()
iwlagn : use vif -> type to check station
rxrpc : Validate the net address given to rxrpc_kernel_begin_call ()
Staging : batman - adv : Remove NULL pointer check
[ media ] em28xx - dvb : Prepare for si2157 driver getting more parameters
OMAP3 : PM : fix lockdep warning caused by omap3_pm_init
[ NETFILTER ]: nf_conntrack_sip : clear address in parse_addr ()
xfs : prevent leaking uninitialized stack memory in FSGEOMETRY_V1
fuse : fix SEEK_END incorrectness
ceph : fix memory leak due to possible dentry init race
ASoC : wm8904 - fix memory leaks
[ media ] r820t : Fix IF scale
drm / radeon / kms : Don ' t kzalloc memory which is immediately overwritten .
drm / i915 : Add lock on drm_helper_resume_force_mode
virt : vbox : fix memory leak in hgcm_call_preprocess_linaddr
drm : when queuing an event with NEXTONMISS , return queued sequence to userspace
mwifiex : Fix possible buffer overflows in mwifiex_ret_wmm_get_status ()
firestream : fix spelling mistake : " reseverd " -> " reserved "
dmaengine : rcar - dmac : use result of updated get_residue in tx_status
ASoC : ssm4567 : Reset device before regcache_sync ()
scsi : megaraid_sas : return error when create DMA pool failed
wlcore : fix unsafe dereference of the wlvif
userns : don ' t leak root user
arch / avr32 : Eliminate NULL test and memset after alloc_bootmem
mm : batch - free pcp list if possible
tproxy : Add missing CAP_NET_ADMIN check to ipv6 side
Btrfs : fix off - by - one in cow_file_range_inline ()
tokenring / 3c359 . c : Prevent possible mem leak when open failed
cifs : return error on invalid value written to cifsFYI
aio : prevent double free in ioctx_alloc
genirq , ARM : Fix boot on ARM platforms
hamradio / yam : fix info leak in ioctl
ARM : S3C64XX : Reduce residency requirement for cpuidle WFI mode
mv643xx_eth : potential null dereference
trace : introduce missing mutex_unlock ()
igmp : Avoid zero delay when receiving odd mixture of IGMP queries
Input : tegra - kbc - fix inverted reset logic
ASoC : SSM2602 : Add entry for the ssm2603 to the device id table
Staging : bcm : add a missing break statement
net : call newid / getid without rtnl mutex held
[ SCSI ] qla2xxx : Correct report - id acquisition check
video : vt8500lcdfb : Remove kfree call since devm_kzalloc () is used
staging : fbtft : do not allocate huge txbuf
ASoC : SOF : ipc : Fix memory leak in sof_set_get_large_ctrl_data
crypto : caam - fix output sequence contiguity check
ALSA : seq : Fix missing NULL check at remove_events ioctl
drm / vc4 : plane : Expand the lower bits by repeating the higher bits
floppy : Add an extra bound check on ioctl arguments
e1000e : static analysis tools complain of a possible null ptr p dereference
NFC : Clear pn533 target structure
HID : cp2112 : fix gpio - callback error handling
USB : EHCI : add a name for the platform - private field
ibm_newemac : Fixes entry of short packets
scsi : Fix use - after - free
mm : Adjust do_pages_stat () so gcc can see copy_from_user () is safe
staging : ozwpan : Return error , if PD is not connected .
ath5k : use tasklet_hi_schedule for beacon queue
dp83640 : Get calibration pin with ptp_find_pin
cdrom : fix improper type cast , which can leat to information leak .
ASoc : wm8731 : add 32bit mode .
fm10k : Fix a potential NULL pointer dereference
x86 / fpu : Don ' t cache access to fpu_fpregs_owner_ctx
greybus : bundle : use hexadecimal notation for class attribute
rtlwifi : rtl8192ce : Set fw_ready flag
ibmvnic : Handle error case when setting link state
batman - adv : fix potential orig_node reference leak
perf hists browser : Fix segfault if use symbol filter in cmdline
KVM : nSVM : Fix IOIO size reported on emulation
tracing : Have error path in predicate_parse () free its allocated memory
ARM : OMAP : rx51 : Fix a section mismatch warn
rsi : release skb if rsi_prepare_beacon fails
cfg80211 : ignore invalid BSSIDs when looking for BSSes
modedb : fix incorrect sync and vmode flags for CVT modes
usb : chipidea : usb2 : check memory allocation failure
USB : allow usbstorage to have LUNS greater than 2Tb
epoll : clear the tfile_check_list on - ELOOP
btrfs : send : remove BUG from process_all_refs
ASoC : Free the card ' s DAPM context
[ media ] sh_veu : v4l2_dev wasn ' t set
mmc : moxart : Handle error from mmc_of_parse ()
staging : vt6655 : device_main : Replace NULL comparison with ! x
[ PATCH ] net / pcnet32 : replace schedule_timeout () with msleep_interruptible ()
crypto : hash - Add AHASH_REQUEST_ON_STACK
drm / ttm : fix memory leak while individualizing BOs
mac80211 : reduce stack usage in sta_ht_capa_read ()
[ media ] omap3isp : Handle omap3isp_csi2_reset () errors
soc : qcom : aoss : Fix missing put_device call in qmp_get
qla2xxx : Wait for reset completion without lock for ISPFX00 .
staging : iio : dac : ad5686 : Check for negative values
ALSA : hda - fix the mic mute led problem for Latitude E5550
drm / nouveau / ltc / gv100 : initial support
ALSA : usb - audio : avoid freeing umidi object twice
target / iblock : Add checks for backend DIF emulation
cpufreq : Prevent problems in update_policy_cpu () if last_cpu == new_cpu
ALSA : snd - usb : Fix uninitialized variable usage
[ ALSA ] ASoC : Fix TLV320AIC3X mono line output interconnect
drm / nouveau / pci / gp104 : initial support
tty : goldfish : Fix a parameter of a call to free_irq
regmap : Check readable regs in _regmap_read
ALSA : timer : Fix leak in events via snd_timer_user_tinterrupt
cifs : fix possible memory corruption in CIFSFindNext
char / tpm : Fix unitialized usage of data buffer
drm / vmwgfx : Fix a buffer object leak
staging : comedi : cb_pcidas : check for failure of subdev_8255_init
mac80211_hwsim : require at least one channel
x86 , efi : Check table header length in efi_bgrt_init ()
ALSA : ali5451 : Fix out - of - bound position reporting
iwlwifi : nvm : don ' t print NVM section reads by default
dmaengine : bcm - sba - raid : serialize dma_cookie_complete () using reqs_lock
staging : goldfish : Call free_irq in error path
ASoC : atmel_ssc_dai : Match the CMR divider only in full duplex .
acpi : add checking for NULL early param
bus : arm - ccn : Do not attempt to configure XPs for cycle counter
ip6gretap : Fix MTU to allow for Ethernet header
xfrm_user : fix info leak in copy_to_user_policy ()
rpmsg : char : release allocated memory
brcmfmac : clear eventmask array before using it
staging : hv : fix counting of available buffer slots when send fails
usb : musb : host : fix for musb_start_urb Oops
mmc : renesas_sdhi_internal_dmac : add R8A77980 to whitelist
USB : usbfs : fix - ENOENT error code to be - ENODEV
[ media ] ov5642 : avoid calling ov5642_find_datafmt () twice
block : handle blk_pm_resume_request () requests in blk_execute_rq_nowait ()
iwlwifi : pcie : fix TVQM queue ID range check
hv_netvsc : fix a race between netvsc_send () and netvsc_init_buf ()
mwifiex : Fix possible buffer overflows in mwifiex_cmd_802_11_ad_hoc_start
defxx . c : dfx_bus_init () is __devexit not __devinit
Yama : handle 32 - bit userspace prctl
crypto : api - Fix crypto_drop_spawn crash on blank spawns
powerpc / powernv : Initialize DMA for PEs
xhci : really enqueue zero length TRBs .
perf tools : Fix a memory leak in __open_dso ()
mac80211 : initialize power mode for mesh STAs
cdrom : Fix info leak / OOB read in cdrom_ioctl_drive_status
staging : rtl8723au : collect_bss_info (): Don ' t memset () what we just kzalloc ()' d
KEYS : Fix termination condition in assoc array garbage collection
drm / amd / display : Add same check as reset pipes for programing backend regs .
mac80211 : fix uninitialized variable
watchdog : Fix null pointer dereference while accessing rdc321x platform_data
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
btrfs : fix in - memory value of total_devices after seed device deletion
crypto : eseqiv - Fix IV generation for sync algorithms
staging : comedi : jr3_pci : change module description
md : allow a partially recovered device to be hot - added to an array .
[ ARM ] dma : convert IOMD DMA to use sg_next ()
rt2x00 : rt2800pci : use rt2x00dev -> tx -> limit
ath9k_hw : Fix tx power settings for AR9003
gpio : davinci : Fix possible NULL pointer deference
rtl8xxxu : Temporarily disable 8192eu device init
[ SCSI ] Fix uninitialised width and speed in sym2
drm / i915 : Fix user ptr check size in eb_relocate_vma ()
iwlwifi : set size of ucode section
[ SCSI ] qla2xxx : Correct regression in relogin code .
ACPI : Add NULL pointer check in acpi_bus_start
rcu : add DEBUG_OBJECTS_RCU_HEAD check for alignment
staging : greybus : uart . c : change ' unsigned ' to ' unsigned int '
staging : wilc1000 : fixes potential null dereference ' wid . val '
qla2xxx : Clear loop_id for ports that are marked lost during fabric scanning .
ax25 : Fix NULL pointer dereference in ax25_kill_by_device
USB : option : reduce interrupt - urb logging verbosity
ALSA : pcm : Call kill_fasync () in stream lock
irqchip / mips - gic : Map to VPs using HW VPNum
Btrfs : fix memory leak in start_transaction ()
usbdev : BKL pushdown
bnxt_en : Do not allow VF to read EEPROM .
staging : unisys : visorbus : visorchipset . c : Fix bug in parser_init_byte_stream .
Page migration : Do not accept invalid nodes in the target nodeset
drm / amdgpu / gfx8 : specify which engine to wait before vm flush
AppArmor : fix oops in apparmor_setprocattr
fs / befs / linuxvfs . c : befs_iget (): remove unneeded initialization to NULL
[ IA64 ] Prevent sn2 ptc code from executing on all ia64 subarches
[ PATCH ] cciss : set default raid level when reading geometry fails
cxgb3 : Missing rtnl lock in error recovery
usb : gadget : don ' t release an existing dev -> buf
ARM : 7809 / 1 : perf : fix event validation for software group leaders
drm / i915 : Annotate user relocs with __user
clk : axi - clkgen : Remove sometimes impossible check
dm : fix the second dec_pending () argument in __split_and_process_bio ()
[ media ] ascot2e : Fix I2C message size check
md : raid1 : kill warning on powerpc_pseries
ceph : prevent i_version from going back
ftrace : iter ctrl fix
drm : Complete moving rotation property to core
clk : hisilicon : hi3660 ： Fix potential NULL dereference in hi3660_stub_clk_probe ()
[ ALSA ] Fix errors with user TLV_WRITE
wl1271 : fix wl1271_spi driver name
jbd2 : clear BH_Delay & BH_Unwritten in journal_unmap_buffer
fs : allow d_instantiate to be called with negative parent dentry
hp - wmi : add return value checking for input_allocate_device ()
io_uring / msg_ring : check file type before putting
x86 : not clear empty_zero_page again
iio : lmp91000 : Set parent device
[ media ] rtl28xxu : lower the rc poll time to mitigate i2c transfer errors
ipv4 : rate limit updating of next hop exceptions with same pmtu
drm / nouveau / fbcon : fix NULL pointer access in nouveau_fbcon_destroy
Input : usb1400_ts - fix access to " device data " in resume function
KVM : async_pf : Add missing call for async page present
fuse : fix uninitialized flags in pipe_buffer
kvm : x86 : fix emulator buffer overflow ( CVE - 2014 - 0049 )
Staging : Octeon - usb : octeon - hcd . c return value
mctp : defer the kfree of object mdev -> addrs
drivers / cpufreq / pcc - cpufreq . c : avoid NULL pointer dereference
HID : roccat : Fixed false dpi reporting when using osd event
phy : ti - pipe3 : Fix suspend / resume and module reload
gma500 : mdfld_dsi_pkg_sender : fix - Wtype - limits warning
vmxnet3 : annotate hwaddr members as __iomem pointers
x86 , mm , hugetlb : Add missing TLB page invalidation for hugetlb_cow ()
fuse : check size of FUSE_NOTIFY_INVAL_ENTRY message
[ S390 ] cio : Log the response from the unit check handler
soc / tegra : pmc : Guard against uninitialised PMC clock
ACPI : debugfs custom_method open to non - root
sysctl : Drop reference added by grab_header in proc_sys_readdir
x86 / microcode / intel : Fix memleak in save_microcode_patch ()
btrfs : verbose error when we find an unexpected item in sys_array
PCI : Fix devm_pci_alloc_host_bridge () memory leak
fscrypto : add authorization check for setting encryption policy
igb : add i211 to i210 PHY workaround
ALSA : HDA : Fixup Realtek headphone pin initialization
mtd : spear_smi : clear status register on init
ath9k : reinitialize block ack window data when starting aggregation
ARM : OMAP : Use kcalloc () in omap_system_dma_probe ()
staging : gdm724x : redundant variables idProduct and idVendor
KVM : nVMX : Validate the IA32_BNDCFGS on nested VM - entry
btrfs : send : use kvmalloc in iterate_dir_item
x86 / mce / AMD : Make the init code more robust
vfio - pci : Add missing break to enable VFIO_PCI_ERR_IRQ_INDEX
phy : tegra : xusb : Fix return value of tegra_xusb_find_port_node function
iio / adc : ( max1363 ) Provide OF node information to iio device
cnic : call cp -> stop_hw () in cnic_start_hw () on allocation failure
drm / nouveau / bios : memset dcb struct to zero before parsing
staging : intel_sst : Fix memory leak
drm / i915 : Restore marking context objects as dirty on pinning
signals , ptrace , sched : Fix a misaligned load inside ptrace_attach ()
ring - buffer - benchmark : Fix the wrong type
userfaultfd : hugetlbfs : remove superfluous page unlock in VM_SHARED case
iwlwifi : mvm : don ' t allow sched scans without matches to be started
xen / evtchn : Fix name of Xen event - channel device
staging : olpc_dcon : actually return the value of i2c_add_driver
input : pmic8xxx - pwrkey : Do not use mfd_get_data ()
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
clk : gpio : Unlock mutex on error path
ARM : DRA7 / AM43XX : fix header definition for omap44xx_restart
pinctrl : sunxi : check clk_prepare_enable return value
[ PATCH ] sil24 : add testing for PCI fault
mtd : nand : don ' t free the global data too early
[ XFS ] Fix a mutex_destroy diagnostic about a locked - mutex - on - destroy from
floppy : silence warning during disk test
brcmfmac : initialize escan function pointer during scheduled scan
e1000e : NVM write protect access removed from SPT HW
fcaps : clear the same personality flags as suid when fcaps are used
[ PATCH ] Dynamic kernel command - line : m68k
usb : otg : twl4030 - usb : Fix unbalanced regulator disables at module removal
orangefs : avoid freeing a slot twice in wait_for_direct_io ()
staging : android : ion : Return an ERR_PTR in ion_map_kernel
ext4 : prevent stack overrun in ext4_file_open
KVM : x86 : Improve thread safety in pit
amdkfd : fence_wait_timeout () can be static
iommu : Stop deferring probe at end of initcalls
mm : compaction : do not compact pgdat for order - 0
futexes : Fix infinite loop in get_futex_key () on huge page
[ media ] cx23885 : Silence unknown command warnings
KVM : MMU : invalidate and flush on spte small -> large page size change
[ media ] imon : fix other RC type protocol support
ASoC : rt5677 : Reintroduce I2C device IDs
wl3501_cs : remove pedantic build warning
staging : slicoss : Fix possible reuse of freed memory in timer function
perf : Treat attr . config as u64 in perf_swevent_init ()
bnx2x : VF ndo sanity
ptp_kvm : probe for kvm guest availability
[ GFS2 ] re - support special inode
rds : prevent dereference of a NULL device
[ PATCH ] powerpc : Fix bug in bug fix for bug in lmb_alloc ()
staging : lustre : race condition for check / use cfs_fail_val
drm / i915 : Disable gpu reset on i965g / gm
uas : Fix bounds check in uas_find_endpoints
drm / vc4 : Fix an integer overflow in temporary allocation layout .
[ libata ] ahci : Fix lack of command retry after a success error handler .
greybus : interface : fix double freeing of interface structure
Btrfs : fix missing error handler if submiting re - read bio fails
powerpc : Fix handling of alignment interrupt on dcbz instruction
mac80211 : fix fragmentation code , particularly for encryption
drm / i915 : Disable tiling on IGDNG for now
IPoIB : Do not print error messages for multicast join retries
qlcnic : Remove __QLCNIC_DEV_UP bit check to read TX queues statistics .
KVM : x86 : Don ' t report guest userspace emulation error to userspace
clk : versatile : free icst on error return
mmc : meson - gx - mmc : Fix platform_get_irq ' s error checking
gpio : moxart : Use devm_gpiochip_add_data () for gpio registration
viafb : remove LVDS initialization
kvm : irqchip : fix memory leak
atmel_spi : reload RCR before TCR
pata_atiixp : fix trivial indentation misalignment on if statement
staging : ks7010 : use u16 instead of unsigned short in hostif_data_indication
sctp : not allow transport timeout value less than HZ / 5 for hb_timer
xen / blkback : Update to use blkdev_get_by_dev instead of open_by_devnum .
mtd_blkdevs : Set the maximum discards size
libata : initialize qc -> dma_dir to DMA_NONE
i40e : Add missing braces to i40e_dcb_need_reconfig ()
xtensa : prevent arbitrary read in ptrace
ieee802154 : ca8210 : prevent memory leak
[ PATCH ] mac80211 : fix GCC warning on 64bit platforms
gro : Fix kcalloc argument order
[ media ] dvb - usb . h function rc5_scan
[ PATCH ] Audit : Fix the format type for size_t variables
driver core : fix device_create () error path
fs / compat_ioctl . c : VIDEO_SET_SPU_PALETTE missing error check
sh : fix copy_from_user ()
perf sched replay : Support using - f to override perf . data file ownership
slub : Use NUMA_NO_NODE in get_partial
io_uring : check that we have a file table when allocating update slots
stmmac : don ' t return zero on failure path in stmmac_pci_probe ()
pinctrl : samsung : Handle memory allocation failure during wakeup banks init
ALSA : hda - Another Dell headset quirk
vfio - pci : Handle error from pci_iomap
sctp : keep fragmentation point aligned to word size
drm / nouveau : require contiguous bo for framebuffer
Btrfs : disk - io . c ( open_ctree ): avoid leaks upon allocation failure
sctp : avoid BUG_ON on sctp_wait_for_sndbuf
atl2 : Disable unimplemented scatter / gather feature
ext4 : fix memory leak in ext4_insert_range ()
percpu : use notrace variant of preempt_disable / preempt_enable
[ PATCH ] fs / smbfs / proc . c : fix data corruption in smb_proc_setattr_unix ()
ALSA : hda - Update automute / automic upon jack retasking
io_uring : fix uninitialized field in rw io_kiocb
[ media ] v4l : async : check for v4l2_dev in v4l2_async_notifier_register ()
mfd : rave - sp : Initialize flow control and parity of the port
tcp : drop SYN + FIN messages
drm / i915 : Do no use kfree () to free a kmem_cache_alloc () return value
Input : ad7879 - pass up error codes from probe functions
perf top : Fix -- delay_secs 0 division by zero
atm : update msg_namelen in vcc_recvmsg ()
bluetooth : Validate socket address length in sco_sock_bind ().
hwmon : Register thermal zone only if ' dev ' parameter was provided
reset : Fix of_reset_control_get () for consistent return values
getname_kernel () needs to make sure that -> name != -> iname in long case
i2c : designware : Use transfer timeout from ioctl I2C_TIMEOUT
i40e : prevent memory leak in i40e_setup_macvlans
libceph : must hold mutex for reset_changed_osds ()
tipc : remove unnecessary call to dev_net ()
x86 / microcode / intel : Guard against stack overflow in the loader
ASoC : imx - pcm - dma : check kzalloc return value in function snd_imx_open
iio : adc : mcp3422 : Checking for error on probe
powerpc / perf : Fix IMC initialization crash
drm / i915 : Expand DPF support to Haswell
dm raid1 : keep retrying alloc if mempool_alloc failed
[ PATCH ] USB : resolve Zaurus problem
mwifiex : pcie : Fix memory leak in mwifiex_pcie_alloc_cmdrsp_buf
USB : pl2303 : fix return value of tiocmset
sdio : handle null tuples
[ SCSI ] mpt2sas : Bump version 08 . 100 . 00 . 00
rtc : ac100 : Add NULL checking for devm_kzalloc call
ceph : check mapping to determine if FILE_CACHE cap is used
ipv6 : Fix ip6_xmit to send fragments if ipfragok is true
[ media ] v4l2 - ioctl : set readbuffers to 2 in g_parm
mnt : release locks on error path in do_loopback
iio : adis16400 : Report pressure channel scale
perf tools : callchain : Fix spurious ' perf report ' warnings : ignore empty callchains
ixgbe : DCB : remove DCB check config
[ XFS ] Make xfsbufd threads freezable
ASoC : Check for NULL register bank in snd_soc_get_cache_val ()
staging : frontier : use after free in disconnect ()
pinctrl / amd : Prepare amd_gpio_irq_handler for irq argument removal
powerpc / 64s / hash : Allow MAP_FIXED allocations to cross 128TB boundary
UBIFS : avoid kernel error if ubifs superblock read fails
af_key : add __GFP_ZERO flag for compose_sadb_supported in function pfkey_register
[ SCSI ] scsi_lib : Fix bug in completion of bidi commands
powerpc / tm : Flush TM only if CPU has TM feature
i2400m : keep index within ms_to_errno []
r8a66597 - hcd : use USB_DT_HUB
iwlwifi : fix compilation warning when CONFIG_IWLWIFI_DEBUG is not set .
[ media ] usbtv : correctly handling failed allocation
tty / serial : at91 : fix race condition in atmel_serial_remove
drm / i915 / gvt : Do not use I915_NUM_ENGINES to iterate over the mocs regs array
[ PATCH ] cpufreq_conservative : alter default responsiveness
[ S390 ] qdio : output queue stall on FCP and network devices
ssb : cc : use correct min_msk for 0x4312
Btrfs : clear uptodate flags of pages in sys_array eb
staging : sbe - 2t3e3 : Fix possible reuse of freed memory in timer function
[ PATCH ] aoe 5 / 12 : don ' t try to free null bufpool
fsl / fman : MEMAC may use QSGMII PHY interface mode
staging : wilc1000 : Revert " staging : wilc1000 : remove spaces around '->'"
[ NETFILTER ]: nf_conntrack_pptp : fix NAT setup of expected GRE connections
iommu / io - pgtable - arm - v7s : Check for leaf entry before dereferencing it
Phonet : refuse to send bigger than MTU packets
ASoC : topology : Fix memory leak in widget creation
staging , rts5208 , LLVMLinux : Change extern inline to static inline
staging : vt6655 : device_rx_srv check sk_buff is NULL
mixart : Add missing vmalloc . h include
drm / i915 : Correctly reject invalid flags for wait_ioctl
USB : gadget : validate interface OS descriptor requests
mmc : dw_mmc : fix the wrong condition checking
af_iucv : exploit target message class support of IUCV
drm / i915 : Start WM computation from scratch on ILK - BDW
ALSA : hda - Add docking station support for another HP machine
drm / etnaviv : ( re -) protect fence allocation with GPU mutex
ASoC : soc - core : use GFP_KERNEL flag for kmalloc in snd_soc_cnew
xfs : disallow ro -> rw remount on norecovery mount
crypto : drbg - fix failure of generating multiple of 2 ** 16 bytes
KVM : s390 : fix assumption for KVM_MAX_VCPUS
qed : Add missing stat for new isles
Staging : rtl8192u : Fix no spaces around '+'
staging : comedi : ni_labpc_cs : remove thisboard macro
[ SCSI ] scsi_scan . c : Release mutex in error handling code
[ PATCH ] swsusp : prevent disks from spinning down and up
tracing : Fix freeing of filter in create_filter () when set_str is false
misc : mei : fix stalled read
PM : Free memory bitmaps if opening / dev / snapshot fails
[ PATCH ] sysfs : zero terminate sysfs write buffers
ipw2100 : declare MODULE_FIRMWARE
drm / i915 : Add fifo underrun reporting state to debugfs
arch / tile : fix reversed test of strict_strtol () return value
rtnl : fix info leak on RTM_GETLINK request for VF devices
drm / i915 : Only flush fbc on sw when fbc is enabled .
drm / i915 : fix integer overflow in i915_gem_execbuffer2 ()
drm / i915 / cnl : Reprogram DMC firmware after S3 / S4 resume
[ SCSI ] ipr : dlpar failed when adding an adapter back
[ PATCH ] lockdep : annotate scheduler runqueue locks
Input : gpio_keys - fix a memory leak
drivers / net / phy / mdio_bus . c : call put_device on device_register () failure
llc : Fix missing msg_namelen update in llc_ui_recvmsg ()
[ PATCH ] stack overflow safe kdump : safe_smp_processor_id (): voyager
proc : prevent accessing / proc /< PID >/ environ until it ' s ready
rose : fix info leak via msg_name in rose_recvmsg ()
tpm : fix a kernel memory leak in tpm - sysfs . c
dm thin : restore requested ' error_if_no_space ' setting on OODS to WRITE transition
usb : phy : msm : cast to enum msm_usb_phy_type
xfs : add missing rmap error return
dccp : Empty the write queue when disconnecting
crypto : user - fix memory leak in crypto_report
ASoC : sgtl5000 : Improve the error message on slave mode setting
skd : Use kmem_cache_free
xen : fix memory leak in __xen_pcibk_add_pci_dev ()
ath9k_htc : Protect ampdu_action with a mutex
iommu / amd : Check if domain is NULL in get_domain () and return - EBUSY
[ PATCH ] drivers cris : return on NULL dev_alloc_skb ()
usb : xhci : Fix OOPS in xhci error handling code
atm : fix info leak in getsockopt ( SO_ATMPVC )
KVM : x86 : apply kvmclock offset to guest wall clock time
ipv6 : sit : update mtu check to take care of gso packets
tty : drop the pty lock during hangup
drivers / net / eql . c : prevent reading uninitialized stack memory
vhost : fix length for cross region descriptor
KVM : x86 : inject exceptions produced by x86_decode_insn
brcmfmac : avoid potential stack overflow in brcmf_cfg80211_start_ap ()
tipc : validate length of sockaddr in connect () for dgram / rdm
fs : umount on symlink leaks mnt count
exofs : Remove redundant unlikely ()
fs_enet : Don ' t receive packets when the napi budget == 0
drm / msm : Call drm_prime_gem_destroy to clean up imported GEM object
USB : uas : Fix slave queue_depth not being set
[ PATCH ] KVM : Add missing ' break '
i386 : arch / x86 / math - emu / fpu_entry . c warning fix
[ CRYPTO ] scatterwalk : Handle zero nbytes in scatterwalk_map_and_copy
clk : tegra : Use definition for pll_u override bit
[ PATCH ] nfsd4 : renew lease on seqid modifying operations
drm / i2c : tda998x : Fix memory leak in tda998x_encoder_init error path .
[ SCSI ] isci : fix missed unlock in apc_agent_timeout ()
ARM64 : PCI : Fix struct acpi_pci_root_ops allocation failure path
xfs : trivial indentation fixup for xfs_iext_remove_node
tg3 : Fix tg3_skb_error_unmap ()
crypto : inside - secure - fix an error handling path in safexcel_probe ()
KVM : x86 : Fix potential divide by 0 in lapic ( CVE - 2013 - 6367 )
wil6210 : fix check for sparrow D0 FW file
tcp : fix use after free in tcp_xmit_retransmit_queue ()
drm / i915 : Fix out - of - bounds array access in bdw_load_gamma_lut
iommu / amd : Free domain id when free a domain of struct dma_ops_domain
powerpc / ptrace : Fix brk . len used uninitialised
[ media ] coda : v4l2 - compliance fix : zero pixel format priv field
coresight : etb10 : Move etb_disable_hw () outside of lock
USB : usbaudio : handle kcalloc failure
drm : Shift wrap bug in create_in_format_blob ()
greybus : audio : return success for stop trigger if device removed
[ PATCH ] x86_64 : free_bootmem_node needs __pa in allocate_aperture
ARM : stm32 : Fix copyright
batman - adv : fix memory leak when dropping packet from other gateway
crypto : blkcipher - Do not use eseqiv on stream ciphers
ipv6 : fix headroom calculation in udp6_ufo_fragment
bnxt_en : Check for zero length value in bnxt_get_nvram_item ().
wm831x_power : Add missing kfree ( wm831x_power ) in wm831x_power_remove ()
drm / virtio : don ' t leak bo on drm_gem_object_init failure
brcmfmac : correct setting of WEP broadcast / unicast keys
drm / i915 : Drain any freed objects prior to hibernation
ALSA : correct kcalloc usage
drm / atomic : Make atomic helper track newly assigned planes correctly , v2 .
ath10k : fix sending wmi cmd during the tdls teardown
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
Btrfs : truncate pages from clone ioctl target range
cpuidle : delay enabling interrupts until all coupled CPUs leave idle
rtc : allow validated RTC_PIE_ON for non - root
crypto : tcrypt - fix uninit sg entries in test_acipher_speed
ipv6 : do not free rt if FIB_LOOKUP_NOREF is set on suppress rule
sysctl : check length at deprecated_sysctl_warning
[ SCSI ] libsas : remove expander from dev list on error
sh64 : re - add the __strnlen_user () prototype
rtc : sun6i : ensure rtc is kfree ' d on error
[ SCSI ] zfcp : Fix error checking for ELS ADISC requests
jbd : Check return value of __getblk ()
cpufreq : st : enable selective initialization based on the platform
gpio - pch : cleanup __iomem annotation usage
mac80211 : mesh only parameter mppath maybe unused
rpmsg : glink : Initialize the " intent_req_comp " completion variable
perf intel - pt : Fix missing stack clear
tracing / filters : don ' t remove old filters when failed to write subsys -> filter
phy : exynos - mipi - video : avoid uninitialized variable use
mtd : spi - nor : improve macronix_quad_enable ()
USB : sierra : fix urb and memory leak in resume error path
drm / amd / display : Do DC mode - change check when adding CRTCs
drm / i915 / gvt : Disable access to stolen memory as a guest
[ PATCH ] readahead : reset cache_hit earlier
USB : serial : upd78f0730 : sort device ids
mfd : don ' t use memzero
cuse : fix memory leak
mac80211 : Fix ibss station got expired immediately
ASoC : fsi : constify dev_pm_ops structure
spi : core : Fix check for dma_map_sg () failures
ext4 : Fix ext4_ext_journal_restart ()
btrfs : fix wrong max system array size check in kernel space
Btrfs : fix wrong extent mapping for DirectIO
iwlwifi : mvm : request statistics when reading debugfs
drm / tegra : Use lockless gem BO free callback
tools / liblockdep : Fix comparison of a boolean value with a value of 2
[ IPV6 ]: Fix slab corruption running ip6sic
[ ALSA ] hda - codec - Fix unbalanced mutex
futex : Prevent overflow by strengthen input validation
ASoC : dapm : Make sure all dapm contexts are updated
ipw2200 : fix error return code in ipw_load ()
KVM : X86 : Don ' t use PV TLB flush with dedicated physical CPUs
drm / bridge : adv7511 : Fix a use after free
ASoC : rockchip : i2s : fix unbalanced clk_disable
staging : gdm72xx : Condense two statements into one to improve code readability .
drm / nouveau / mmu / gp104 : initial support
usb : gadget : s3c2410_udc : Remove unneeded condition
staging / lustre / llite : Fix integer overflow in ll_fid2path
cxgb4vf : Fail open if link_start () fails .
Drivers : w1 : remove last __devexit_p () instance
mm : hugetlb : fix clear_huge_page ()
hwmon : ( ina2xx ) don ' t accept shunt values greater than the calibration factor
ASoC : AMD : Add NULL pointer check
perf tools : Fix new_term () missing free on error path
drm / nouveau / fifo / gp104 : initial support
[ X25 ]: Restore skb -> dev setting in x25_type_trans ().
rxrpc : Fix oops when discarding a preallocated service call
sh : Fixed the TMU0 reload value on resume
usb : dwc3 : gadget : add a debugging print when initializing endpoints
mm : Introduce kvcalloc ()
Input : ams_delta_serio - fix wrong kfree in ams_delta_serio_exit
Btrfs : add missing compression property remove in btrfs_ioctl_setflags
ALSA : timer : Fix double unlink of active_list
ovl : fix dentry reference leak
[ media ] dvbdev : replace kcalloc with kzalloc
panic , vt : do not force oops output when panic_timeout < 0
sunvnet : fix NULL pointer dereference
ASoC : cs42l52 : microphone bias is controlled by IFACE_CTL2 register .
microblaze : Remove duplicated prototype of start_thread ()
i2c - omap : Fix build breaking typo cpu_is_omap_2430
mac80211 : fix the assignment of mesh element TTL
[ XFS ] Fix up warning for xfs_vn_listxatt ' s call of list_one_attr () with
mac80211 : use RCU read locks for sta_info_get
mwifiex : fix a reversed condition
tpm / st33zp24 / spi : Improve st33zp24_spi_evaluate_latency
exit : proc : don ' t try to flush / proc / tgid / task / tgid
KVM : Improve create VCPU parameter ( CVE - 2013 - 4587 )
usb : otg : mxs - phy : add missing type to usb_phy
usb : gadget : fsl : fix the fault issue on rmmod
staging : lustre : llite : default dir stripe index only for mkdir
ASoC : qcom : ipq806x : add error in dma allocation .
mac80211 : fix AP - mode frame matching
sm501fb : deallocate colormap only if allocated
staging : sep : add missing misc_deregister () on error in sep_register_driver_with_fs ()
virt : vbox : Only copy_from_user the request - header once
ath10k : fix firmware assert in monitor mode
[ PATCH ] Char : serial167 , remove useless tty check
pinctrl : sx150x : handle missing ' advanced ' reg in sx1504 and sx1505
x86 : fix pci - gart failure handling
tpm_infineon : remove use of __devexit_p
ima : fix add LSM rule bug
[ CIFS ] fix trailing whitespace
cfg80211 : initialize sinfo in cfg80211_get_station
KVM : Align cr8 threshold when userspace changes cr8
igb : add conditions for I210 to generate periodic clock output
be2iscsi : add an missing goto in error path
soc / tegra : pmc : Ensure mutex is always initialised
PCI : cpqphp : Fix possible null pointer dereference
perf tools : Fix segfault when using srcline sort key
Btrfs : fix oops when btrfs_inherit_iflags called with a NULL dir
tracing / filters : fix off - by - one bug
[ media ] Don ' t OOPS if videobuf_dvb_get_frontend return NULL
staging : octeon : Fix warning of prefer ether_addr_copy .
sfc : insert ARFS filters with replace_equal = true
igb : fix vf lookup
[ media ] vivi : set device_caps
ASoC : Return correct codec clock in s3c64xx - i2s
sysctl : Add neg_one as a standard constraint
AFS : Fix afs_prepare_write ()
UBIFS : return EROFS in case of broken commit
[ media ] saa7164 : better handle error codes
tcp : ipv6 : bind () use stronger condition for bind_conflict
i40iw : Add NULL check for ibqp event handler
xen : events : do not free legacy IRQs
[ media ] gspca : reset image_len to 0 on LAST_PACKET when discarding frame
crypto : ixp4xx - don ' t leak pointers to authenc keys
ARM : EXYNOS : fix GIC using for EXYNOS5440
greybus : module : name routines consistently
sysfs : fix hardlink count on device_move
crypto : algif_aead - fix reference counting of null skcipher
arm64 : Fix building error with 16KB pages and 36 - bit VA
ACPI : add AC / DC notifier
ASoC : Fix register cache initialisation for WM8753
fs / 9p : destroy fid on failed remove
mac80211 : Fix frame injection using non - AP vif
memcg : fix wrong check of noswap with softlimit
drm / amd / display : Set irq state only on existing crtcs
drm / i915 : use DPLL_DVO_HIGH_SPEED for PCH eDP
dmaengine : device must have at least one channel
NVMe : End sync requests immediately on failure
tools : iio : generic_buffer : free dev_dir_name on exit
f2fs : check range before defragment
mac80211 : fix rate control initialisation
bnx2x : fix possible panic under memory stress
asus - laptop : fix a memory leak in asus_laptop_get_info error path
Input : synaptics - add manual min / max quirk for ThinkPad X240
drivers / atm / eni . c : ensure arguments to request_irq and free_irq are compatible
drm / i915 : Clear intel_crtc -> atomic before updating it .
ACPI : silence kmemcheck false positive
ASoC : wm_adsp : Fix support for firmware switching on DSP2 and higher
ipv4 : Do not use dead fib_info entries .
kvm : x86 : make function static to avoid compiling warning
mm : vma_adjust : remove superfluous check for next not NULL
ALSA : echoaudio : check kmalloc () result
imx - drm : imx - ldb : Check ' mode ' pointer first
[ PATCH ] x86_64 : Don ' t confuse apic =... command line option with apic
ARC : fix build warnings with ! CONFIG_KPROBES
iwlwifi : mvm : reduce active dwell time
sfc : Use kzalloc () to ensure struct efx_spi_device is fully initialised
[ SCSI ] ipr : Always initiate hard reset in kdump kernel
perf / x86 / intel / uncore : Correct fixed counter index check in generic code
mm : check that we have the right vma in __access_remote_vm ()
ARM : davinci : da830 - evm : specify the chipselect in davinci_nand_pdata
dmaengine : sun6i - dma : Only calculate residue if state exists .
bnx2x : Add known PHY type check
Btrfs : don ' t run __tree_mod_log_free_eb on leaves
seq_file : disallow extremely large seq buffer allocations
btrfs : Check name_len before reading btrfs_get_name
dmaengine : k3dma : Off by one in k3_of_dma_simple_xlate ()
ath9k : release allocated buffer if timed out
acornfb : Dont BUG () on invalid pan parameters
[ IPV6 ]: Address autoconfiguration does not work after device down / up cycle
mfd : max77687 : Add registration of max77686 - clk
mailbox : add MODULE_LICENSE () for mtk - cmdq - mailbox . c
fbcon : when font is freed , clear also vc_font . data
HID : sensor - hub : fix potential memory leak
video : wm8505fb : Remove redundant of_match_ptr
fs : Add a missing permission check to do_umount
[ MTD ] MTD_DEBUG always does compile - time typechecks
[ SCSI ] megaraid_sas : remove sysfs dbg_lvl world writeable permissions
crypto : ccp - Release all allocated memory if sha type is invalid
drm / i915 : release mutex in i915_gem_init ()' s error path
s390 / cio : add NULL test
ALSA : HDA : Fix Oops caused by dereference NULL pointer
pinctrl : imx1 : fix return value check in imx1_pinctrl_core_probe ()
ARM : 6891 / 1 : prevent heap corruption in OABI semtimedop
drm / i915 / bxt , glk : Fix assert on conditions for DC9 enabling
tile : array underflow in setup_maxnodemem ()
tracing / filter : Add missing initialization
x86 / intel_rdt : Remove redundant ternary operator on return
sysfs : Make poll behaviour consistent
staging : dgap : Add check for MAXBOARDS in . probe ( dgap_init_one )
drivers / perf : arm_pmu_acpi : Release memory obtained by kasprintf
exofs : use bio_clone_fast in _write_mirror
xfs : fix missing ILOCK unlock when xfs_setattr_nonsize fails due to EDQUOT
GFS2 : Fix freeze issue
sch_sfb : Fix missing NULL check
unix : Use FIELD_SIZEOF () in af_unix_init ().
ASoC : Intel : Skylake : check manifest size
[ media ] drivers / media / dvb - frontends / stv0900_sw . c : Fix break placement
rt2x00 : Don ' t count retries as failure
apparmor : fix update the mtime of the profile file on replacement
drm / i915 : fix up the relocate_entry refactoring
perf trace / scripting : Check return val of perl_run ()
qlcnic : Fix function return error check
atm : fix info leak via getsockname ()
[ PATCH ] Bluetooth : fix potential NULL ptr deref in dtl1_cs . c :: dtl1_hci_send_frame ()
perf / core : Update filters only on executable mmap
sched : Remove unlikely () from ttwu_post_activation
tracing : Fix off - by - one in trace_get_user ()
proc : fix null pointer deref in proc_pid_permission ()
drivers / staging / cx25821 / cx25821 - video . c : introduce missing kfree
staging : vt6656 : check ieee80211_bss_conf bssid not NULL
tcp : Set CHECKSUM_UNNECESSARY in tcp_init_nondata_skb
inotify : stop kernel memory leak on file creation failure
ethtool : EEPROM dump no longer works for tg3 and natsemi
PCI : Use cpu_hotplug_disable () instead of get_online_cpus ()
[ media ] vb2 : fix compiler warning
debugfs : set no_llseek in DEFINE_DEBUGFS_ATTRIBUTE
ALSA : als4000 : enable burst mode
[ SCSI ] sr : remove dead code
Btrfs : Fix test_range_bit for whole file extents
[ NETFILTER ]: nfnetlink_log : fix NULL pointer dereference
evm : checking if removexattr is not a NULL
ASoC : wm8753 : remove invalid free of devm_ allocated data
vmwgfx : Unreference surface on cursor error path
ceph : fix erroneous cap flush to non - auth mds
goldfish_pipe : use GFP_ATOMIC under spin lock
mwl8k : prevent freeing free IRQ if ieee80211_register_hw () fails
ASoC : Intel : Skylake : Initialize module list for Broxton
UWB : Return UWB_RSV_ALLOC_NOT_FOUND rather than crashing on NULL dereference if kzalloc fails
xfrm : Fix kernel panic when flush and dump SPD entries
ACPI : ioapic : Clear on - stack resource before using it
crypto : atmel - aes - add new version
[ PATCH ] HID read busywait fix
brcmfmac : Add check for short event packets
i2c : mux : reg : wrong condition checked for of_address_to_resource return value
lib : rhashtable : Correct self - assignment in rhashtable . c
mmc : core : Add card_busy to host_ops
Btrfs : fix race between readahead and device replace / removal
orinoco : Maintain lock until entry removed from list
iio : gyro : ADXRS450 : Add missing read support for calibbias attribute
Staging : otus : hpfwu . c : Checkpatch cleanup
staging : wilc1000 : add check for kmalloc allocation failure .
make macfb_setup () static
[ SCSI ] libsas : Warn if ATA device detected but CONFIG_SCSI_SAS_ATA not set
tipc : fix an infoleak in tipc_nl_compat_link_dump
x86 / ioapic : Prevent NULL pointer dereference in setup_ioapic_dest ()
KEYS : fix out - of - bounds read during ASN . 1 parsing
staging / xgifb : Fix return of uninitialized variable
iio : adc : set INPUT_PROP_DIRECT
cifs : potential missing check for posix_lock_file_wait
rpmsg : virtio : Fix possible double free in rpmsg_virtio_add_ctrl_dev ()
rxrpc : Validate the net address given to rxrpc_kernel_begin_call ()
dm crypt : fix free of bad values after tfm allocation failure
ixgbe : Fix FCOE memory leak for DDP packets
[ agp ]: fixup chipset flush for new Intel G4x .
crypto : authenc - Use correct ahash complete functions
drm / radeon / kms : Don ' t kzalloc memory which is immediately overwritten .
device property : export irqchip_fwnode_ops
scsi : scsi_transport_iscsi : fix the issue that iscsi_if_rx doesn ' t parse nlmsg properly
pinctrl : imx : off by one in imx_pinconf_group_dbg_show ()
f2fs : skip writing in -> writepages when no dirty pages exist
iwlwifi : mvm : comply with fw_restart mod param on suspend
drm / radeon : fix UVD 256MB check
OMAP3 : SRAM size fix for HS / EMU devices
btrfs : fix missing last - entry in readdir ( 3 )
mmc : sdhci - st : explicitly request exclusive reset control
ASoC : Hold client_mutex while calling snd_soc_instantiate_cards ()
kconfig / nconf : prevent segfault on empty menu
HID : logitech : fix bounds checking on LED report size
drivers / net / pcmcia / 3c589_cs : fix port configuration switcheroo
sc16is7xx : enable the clock
UDF : Fix a null pointer dereference in udf_sb_free_partitions
drm / bridge : synopsys : dw - hdmi : clean up drm_bridge_add call
mtd : orion_nand : Fix build failure caused by typo
iucv : Fix missing msg_namelen update in iucv_sock_recvmsg ()
efi / fb : Correct PCI_STD_RESOURCE_END usage
x86 : x86 CORE_DUMP_USE_REGSET
fs : logfs : Fix up MTD = y build .
hfsplus : fix Buffer overflow with a corrupted image
spi : zynq : missing break statement
isicom : Deinline drop_dtr , save 112 bytes
Btrfs : make sure btrfs_remove_free_space doesn ' t leak EAGAIN
[ media ] pvrusb2 : don ' t go past buf array
[ PATCH ] autofs4 : need to invalidate children on tree mount expire
staging : comedi : das16 : change MODULE_DESCRIPTION
Btrfs : fix memory leak in btrfs_ioctl_start_sync ()
Btrfs : Don ' t error on resizing FS to same size
PCI : tegra : Fix host bridge memory leakage
drm : stop information leak of old kernel stack .
drm / vc4 : Return - EINVAL on the overflow checks failing .
[ PATCH ] epca : update . owner field of struct pci_driver
drm / atmel - hlcdc : check stride values in the first plane
[ media ] davinci : vpbe : fix build error when CONFIG_VIDEO_ADV_DEBUG is enabled
rtc : m41t80 : add wakealarm functionality
wcn36xx : Fix error handling
stmmac : Remove unbalanced clk_disable call
mmc : mmci : Cascade EPROBE_DEFER from regulators .
staging : wilc1000 : fix parameter name of function declaration
drm / nouveau / disp / nv04 : avoid creation of output paths
HID : hidraw : Fix crash on HIDIOCGFEATURE with a destroyed device
mfd : arizona : Check errors from regcache_sync ()
btrfs : check file extent backref offset underflow
iptunnel : make rx / tx bytes counters consistent
dmaengine : Stricter legacy checking in dma_request_slave_channel_compat ()
btrfs : send : silence an integer overflow warning
Input : sur40 - fix error return code
mmc : core : Fix use of uninitialized data in mmc_send_if_cond .
HID : hid - picolcd : Fix memory leak in picolcd_debug_out_report ()
ath6kl : Support net_stats . multicast
mac80211 : fix rcu locking
IPoIB : Fix crash in path_rec_completion ()
linux / types . h : Use __u64 for aligned_u64
[ SCSI ] be2iscsi : memset wrb for ring create
Input : twl6040 - vibra - do not reparent to grandparent
drm / i915 : Don ' t try to calculate RC6 residency on GEN4 and before
Btrfs : Add error handle for btrfs_search_slot () in btrfs_read_chunk_tree ()
fib_semantics : Don ' t match route with mismatching tclassid
sunrpc : Avoid a KASAN slab - out - of - bounds bug in xdr_set_page_base ()
parisc / PCI : Set type for LBA bus_num resource
drm / edid : Don ' t skip every eighth EST III mode
GFS2 : O_TRUNC not working on stuffed files across cluster
platform / x86 : intel_ips : Use PCI_VDEVICE () macro
powerpc / cell : Fix integer constant warning
HID : hid - cypress : validate length of report
DLM : fix overflow dlm_cb_seq
scsi : lpfc : Fix NULL pointer dereference during PCI error recovery
ax25 : fix NPD bug in ax25_disconnect
perf / core : Fix perf_uprobe_init ()
[ PATCH ] ieee1394 : fix cross_bound check for null ISO packets
crypto : md5 - Set statesize
drm / atomic : Handle funcs -> best_encoder == NULL case
include / linux / poison . h : fix LIST_POISON { 1 , 2 } offset
xen / blkfront : Make sure that the device is fully ready before allowing release .
[ SCSI ] ibmvscsi : Add 16 byte CDB support
mac80211 : read station mgmt keys via get_key call
i40e : keep service tasks out of reset process
fs / select . c : fix information leak to userspace
ovl : Return - ENOMEM if an allocation fails ovl_lookup ()
[ MIPS ] Fix oprofile module unloading
[ SCSI ] qla4xxx : a small loop fix
btrfs : fix NULL pointer dereference when deleting device by invalid id
ocfs2 : free path in ocfs2_remove_inode_range ()
drm / amd / display : Correct fixed point calculation .
module : don ' t call percpu_modfree on NULL pointer .
PCI : xilinx : Dispose of MSI virtual IRQ
scsi : sg : reset ' res_in_use ' after unlinking reserved array
ASoC : imx - ssi : add check on platform_get_irq return value
drm / exynos : fix flags in dma buf exporting
perf evlist : Return first evsel for non - sample event on old kernel
ASoC : topology : Add support for compressed PCMs
drm / i915 : Clear the pending_gpu_fenced_access flag at the start of execbuffer
xfs : simple btree query range should look right if LE lookup fails
coresight : fix kernel panic caused by invalid CPU
Staging : speakup : free after input_unregister_device ()
libata piix3 support warning fix
staging : fsl - dpaa2 / eth : Check if notification rearm is successful
Btrfs : Fix balance panic
staging : comedi : das16m1 : check for subdev_8255_init () failure
usb : gadget : f_ecm : fix missing unlock on error in ecm_alloc ()
wl12xx_sdio : set interrupt as wake_up interrupt
drbd : abort drbd_start_resync if there is no connection
md : make recovery started by do_md_run () visible via sync_action
usb : gadget : mv_udc : fix readl error
x86 / topology : Handle CPUID bogosity gracefully
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
cfg80211 : fix indentation
shm : optimize exit_shm ()
Fix a dumb typo - use of & instead of &&
drm : missing drm_vm . c changes for consistent maps
mmc : sdhci : fix low memory corruption
dmaengine : ste_dma40 : off by one in d40_of_probe ()
usb : message : Fixed parenthesis error in sizeof function .
msm : timer : fix missing return value
drm / i915 : Initialize return value for empty i915_gem_object_unbind ()
batman - adv : remove obsolete skb_reset_mac_header () in batadv_bla_tx ()
HID : Add mapping of new KEY_MEDIA_REPEAT keycode
skge : Adjust a null pointer check in skge_down ()
rds : fix an infoleak in rds_inc_info_copy
[ media ] dvb : si21xx : buffer overflow in si21_writeregs ()
usb : storage : return on error to avoid a null pointer dereference
vmscan : promote shared file mapped pages
wanxl : fix info leak in ioctl
isdn : avoid copying too long drvid
USB : yurex : Fix missing URB_NO_TRANSFER_DMA_MAP flag in urb
perf / x86 : Correctly use FEATURE_PDCM
[ MIPS ] klconfig add missing bracket
i2c : ismt : prevent memory corruption in ismt_access ()
Btrfs : avoid warning bomb of btrfs_invalidate_inodes
cpufreq : sfi : Don ' t validate the frequency table twice
drm : bochs : Prevent double - free of fb helper
genirq : Fix type inconsistency
[ PATCH ] fuse : handle error INIT reply
i7core_edac : do not export static functions
staging : most : fix sparse warning Using plain integer as NULL pointer
[ PATCH ] USB : check for device in zd1201_resume
mtd : spi - nor : Off by one in cqspi_setup_flash ()
ALSA : hda - Skip card registration when no codec is found
drivers / net / fs_enet : remove null pointer dereference
[ PATCH ] ISDN : Avoid a potential NULL ptr deref in ippp
PCI : pciehp : disable DLL state changed event notification
[ PATCH ] knfsd : Protect update to sn_nrthreads with lock_kernel
ipv4 : fix debug info in tnode_new
wimax : i2400 : Fix memory leak in i2400m_op_rfkill_sw_toggle
iscsi - target : missing kfree () on error path
[ PATCH ] i2c : chips / ds1374 . c fixup
reiserfs : Fix softlockup while waiting on an inode
mac80211 : Clear wowlan flag when drv_suspend returns failure
Blackfin : debug - mmrs : disable PERIPHERAL_MAP for IMDMA channels
[ media ] solo6x10 : Fix solo_eeprom_read retval type
ASoC : sgtl5000 : defer the probe if clock is not found
btrfs : add check to sysfs handler of label
KVM : use after free in kvm_ioctl_create_device ()
Input : wm97xx - core - use IRQF_SAMPLE_RANDOM
drm / i915 : Initialise g4x watermarks for disabled pipes
sched : suppress RCU lockdep splat in task_fork_fair
drm / i915 : Avoid NULL ctx -> obj dereference in debugfs / i915_context_info
UBIFS : replace count * size kzalloc by kcalloc
cxlflash : off by one bug in cxlflash_show_port_status ()
qla2xxx : Update entry type 270 to match spec update .
crypto : shash - Fix unaligned calculation with short length
UBI : Silence an unintialized variable warning
arm / arm64 : KVM : Don ' t allow creating VCPUs after vgic_initialized
video : uvesafb : Fix integer overflow in allocation
drm / i915 / userptr : reject zero user_size
lockdep : fix overflow in the hlock shrinkage code
xfrm : Restrict extended sequence numbers to esp
ucc_geth : Add SUPPORTED_MII and SUPPORTED_Autoneg
dccp : do not send reset to already closed sockets
pinctrl / amd : Drop pinctrl_unregister for devm_ registered device
rds : memory leak in __rds_conn_create ()
timekeeping : Make local variables static
mtd : onenand : omap2 : Remove skip initial unlocking support
UBI : always re - read in case of read failures
clk : st : Fix memory leak
ibmvnic : Fix NAPI structures memory leak
[ SCSI ] megaraid : Use the proper type to hold the irq number .
Bluetooth : Release BTM while sleeping to avoid deadlock
staging : comedi : unionxx5 : fix memory leak during subdevice init
crypto : algif_skcipher - sendmsg SG marking is off by one
staging : comedi : cb_das16_cs : return ' 0 ' for successful attach
farsync : fix info leak in ioctl
tg3 : Update version to 3 . 109
[ ALSA ] aoa : platform function gpio : ignore errors from functions that don ' t exist
vfs : rename : check backing inode being equal
ocfs2 : Fix xattr check in ocfs2_get_xattr_nolock ()
tty : release_one_tty () forgets to put pids
ALSA : HDA : Fix internal microphone on Dell Studio 16 XPS 1645
amd_iommu : do not dereference a NULL pointer address .
vfs : extend vfs_removexattr locking
misc : mic : Fix for double fetch security bug in VOP driver
video : goldfishfb : fix memory leak on driver remove
TTY : drop driver reference in tty_open fail path
iio : inkern : allocate zeroed memory
drbd : Becoming sync target may not happen out of < C_WF_REPORT_PARAMS
writeback : avoid unnecessary determine_dirtyable_memory call
drm / i915 : enable HDMI mode on VLV when an HDMI sink is detected
fbdev : sm501fb : use memset_io
watchdog : davinci : Handle return value of clk_prepare_enable
wil6210 : Improve AP stop handling
mwifiex : using right aid value for tdls action frame
mac80211 : fix ieee80211_sta_process_chanswitch for 5 / 10 MHz channels
fix grammar in printk
sched : fix memory leak in a failure path
ALSA : compress_core : integer overflow in snd_compr_allocate_buffer ()
usb : dwc2 : avoid leaking DMA channels on disconnection
platform / mellanox : mlxreg - hotplug : Fix uninitialized variable
pinctrl : sunxi : Enforce the strict mode by default
iio : imu : adis16400 : release allocated memory on failure
[ PATCH ] moxa serial : add proper capability check
hwmon : ( emc1403 ) Support full range of known chip revision numbers
cpufreq : SPEAr : clk_round_rate () can return a zero upon error
dlm : Fix kernel memory disclosure
drm / i915 : Don ' t overrun the intel_wa_regs array
hwmon : ( scmi ) handle absence of few types of sensors
ath9k_hw : reduce delay on programming INI on AR9003
vfs : mnt_want_write_file (): fix special file handling
Btrfs : only do the tree_mod_log_free_eb if this is our last ref
ipv4 : check optlen for IP_MULTICAST_IF option
HID : enable hid device to suspend / resume asynchronously
kernel / signal . c : stop info leak via the tkill and the tgkill syscalls
libnvdimm , pmem : fix persistence warning
x86 / irq : Fix move_irq_desc () for nodes without ram
cxgb4 : Inform caller if driver didn ' t upgrade firmware
staging : ks7010 : change some local variables type in ks_sdio_interrupt
xprtrdma : Fix DMA - API - DEBUG warning by checking dma_map result
[ S390 ] cio : path_event overindication after resume
[ PATCH ] pcnet32 : remove incorrect pcnet32_free_ring
f2fs : fix a dead loop in f2fs_fiemap ()
[ PATCH ] e1000 : Enable polling before enabling interrupts
staging : rtl8188eu : Use kcalloc instead of kzalloc .
[ PATCH ] uml : switch_mm fix
drm / amdgpu_gem : fix error handling path in amdgpu_gem_va_update_vm
netrom : fix info leak via msg_name in nr_recvmsg ()
ALSA : usb - audio : add DSD support for new Amanero PID
TG3 : limit reaches - 1
[ media ] media - device : fix infoleak in ioctl media_enum_entities ()
[ media ] easycap : Fix mem leak in easycap_usb_probe ()
brcmfmac : Fix crash on cleanup .
[ media ] tvp5150 : allow get / set_fmt on the video source pad
ubi : Fix whitespace issue in count_fastmap_pebs ()
drm / i915 : Prevent using uninitialized MMIO funcs
Btrfs : skip commit transaction if we don ' t have enough pinned bytes
[ ALSA ] soc - Clean up tabs
drm / i915 : Missed lock change with rps lock
USB : fix dereference in drivers / usb / misc / adutux . c
proc : prevent stacking filesystems on top
USB : core : harden cdc_parse_cdc_header
soc : ti : knav : Add a NULL pointer check for kdev in knav_pool_create
staging : iio : isl29018 : fix sparse warning regarding incorrect type ( different signedness )
iio : st_pressure : Export sampling frequency for lps25h and lps331ap
ibmvnic : Free tx / rx scrq pointer array when releasing sub - crqs
ipvs : add check in ftp for initialized core
bonding : disable netpoll on enslave failure
[ media ] media : platform : s3c - camif : Constify platform_device_id
i40e : Fix memory leak at failure path in i40e_dbg_command_write ()
x86 , e820 : Guard against array overflowed in __e820_add_region ()
perf report : Compare symbol name for inlined frames when sorting
svcrdma : Add put of connection ESTABLISHED reference in rdma_cma_handler
uprobes : Allocate -> utask before handler_chain () for tracing handlers
ath9k : Do not use local_bh_disable in ampdu_action
blkcg : fix double free of new_blkg in blkcg_init_queue
scsi : hpsa : do not reset enclosures
ext4 : fix deadlock with quota feature
drm / msm / hdmi : redefinitions of macros not required
xfrm_user : fix info leak in copy_to_user_tmpl ()
[ ARM ] fix leak in iop13xx / pci
cyclades : fix read buffer overflow
hwmon : ( asb100 ) Fix vrm write operation
[ DLM ] Fix memory leak in dlm_add_member () when dlm_node_weight () returns less than zero
drivers : net : slip : fix NPD bug in sl_tx_timeout ()
vesafb : fix memory leak
USB : cdc - acm tell tty layer not to split things up .
[ SPARC ]: Add missing of_node_put
KVM : arm / arm64 : vgic : Fix kvm_vgic_map_is_active ' s dist check
KVM : emulate : avoid accessing NULL ctxt -> memopp
Staging : rtl8188eu : usb_halinit : Use ! x instead of x == NULL .
ext4 : check for zero length extent
[ TCP ]: Fix TCP header misalignment
mfd : twl6040 : Check for error when reading revision register
mtd : lpc32xx_slc : Make probe () return - EPROBE_DEFER if necessary
lib / decompress : set the compressor name to NULL on error
USB : serial : console : fix use - after - free on disconnect
fscrypt : fix renaming and linking special files
staging : fbtft : Fix potential null dereference
embedded : fix vc_translate operator precedence
be2net : fix pmac_id for BE3 VFs
drm / i915 / execlists : Add a GEM_TRACE to show when the context is completed
greybus : hd : fix cport - count check
phonet : refcount leak in pep_sock_accep
sunrpc : integer underflow in rsc_parse ()
mm : Fix NULL pointer dereference in madvise ( MADV_WILLNEED ) support
selinux : fix kernel BUG on prlimit (..., NULL , NULL )
drm / i915 / sdvo : Cleanup connector on error path
libceph : delete from need_resend_linger before check_linger_pool_dne ()
pinctrl : pinconf : fix comparison of different types
[ XFS ] fix valid but harmless sparse warning
drm / nouveau / devinit / nv04 : change owner to int
rpmsg : virtio : Fix possible double free in rpmsg_probe ()
x86 / apic / irq_remap : Silence a bogus pr_err ()
Staging : ced1401 : Fixes trailing whitespace checkpath warning .
mac80211 : fix register_hw error path
drm / i915 : Change WARN to ERROR in CSB count
kcm : fix a null pointer dereference in kcm_sendmsg ()
[ PATCH ] ia64 : add ptr_to_compat ()
dmapool : fix overflow condition in pool_find_page ()
drm / nouveau / bios / gp104 : initial support
greybus : svc : Pass the correct pointer to input_free_device ()
[ media ] ttusb - dec : buffer overflow in ioctl
xfs : fix quotacheck dquot id overflow infinite loop
ipmi : fix use - after - free of user -> release_barrier . rda
drm / msm / mdp5 : make helper function static
ASoC : rockchip : Enable 192khz in hw_params operation
USB : cdc - acm : more sanity checking
ASoC : Fix incorrect kfree in wm8731_probe error path
ALSA : usb - audio : Fix out - of - bound error
ASoC : Fix reg_word_size for ak4104
ext4 : do not use stripe_width if it is not set
iio : core : Handle error when mask type is not separate
drm / i915 : Fix CSR MMIO address check
[ media ] lirc : return ENOTTY when device does support ioctl
i40iw : Fix double free of allocated_buffer
clk : hisilicon : Use kcalloc () in hisi_clk_init ()
crypto : kpp - add get / set_flags helpers
[ XFS ] Fix realtime subvolume expansion , a porting bug b0rked it . Coverity
atm : ambassador : use NULL instead of 0 for pointer
pty : fix data loss when stopped (^ S /^ Q )
ipvs : allow rescheduling after RST
staging : ks7010 : change local variable type in ks_wlan_hw_rx
virtio : fix memory leak in virtqueue_add ()
drbd : fix possible access after free
staging : unisys : delete extra space character in vbuschannel_itoa ()
ext4 : fix potential integer overflow
ASoC : Instantiate card widgets immediately
thinkpad_acpi : Fix a memory leak during module exit
fs : clear writeback errors in inode_init_always
KVM : PIT : control word is write - only
mac80211 : reset CQM history upon reconfiguration
NFS : Clean up : revert increase in READDIR RPC buffer max size
drivers / net / wireless / b43 / main . c : 4351 : Fixed coding style
reiserfs : Don ' t call reiserfs_get_acl () with the reiserfs lock
HID : hidraw , fix a NULL pointer dereference in hidraw_ioctl
TTY : hvcs : fix missing unlock on error in hvcs_initialize ()
mfd : Add free max8998 -> ono irq in max8998_irq_exit ()
KVM : PPC : Book3S : Check KVM_CREATE_SPAPR_TCE_64 parameters
mac80211 : fix conn_mon_timer running after disassociate
kernel / relay . c : fix potential memory leak
regmap : Silence warning on invalid zero length read
ASoC : tlv320aic3x : Don ' t sync first two registers from register cache
s390 / qeth : fix thinko in IPv4 multicast address tracking
ALSA : control : Make sure that id -> index does not overflow
ibmvnic : fix missing unlock on error in __ibmvnic_reset ()
ALSA : hda - Cirrus : Correctly clear line_out_pins when moving to speaker
bdi : register sysfs bdi device only once per queue
dmaengine : pl330 : do not emit loop for 1 byte transfer .
x86 - 64 : Fix the failure case in copy_user_handle_tail ()
ata_piix : add short 40c quirk for Acer Aspire 2030 , take # 2
drm / i915 : Hold rcu_read_lock when iterating over the radixtree ( vma idr )
tg3 : Fix 5906 link problems
malidp : Fix NULL vs IS_ERR () checking
usb : mon : make mmapped memory read only
ALSA : hda - Fix auto - mic detection in Conexant codec - parser
ext4 : verify extent header depth
mac80211 : fix a possible leak of station stats
imx - drm : imx - drm - core : improve safety of imx_drm_add_crtc ()
ceph : fix cap_wanted_delay_ { min , max } mount option initialization
arm64 : make sys_call_table const
HID : fix error condition propagation in hid - sony driver
ipv6 : udp : fix the wrong headroom check
staging / dgnc : fix info leak in ioctl
tcp : fix TSO FACK loss marking in tcp_mark_head_lost
[ ALSA ] sound / isa / gus / interwave . c : check kmalloc () return value
UBI : mtd / ubi / vtbl . c : fix memory leak
batman - adv : randomize initial seqno to avoid collision
iio : st_sensors : make detection more helpful
misc : fastrpc : prevent memory leak in fastrpc_dma_buf_attach
[ IPV6 ] ROUTE : Prefer reachable nexthop only if the caller requests .
dmaengine : zynqmp_dma : Fix race condition in the probe
Btrfs : fix extent_same allowing destination offset beyond i_size
fs / cifs / cifs_dfs_ref . c : fix potential memory leakage
scsi : libiscsi : Fix indentation
rpmsg : Only invoke announce_create for rpdev with endpoints
ipmi : Fix memory leak in __ipmi_bmc_register
staging : iio : lpc32xx_adc : Fix IRQ check
aacraid : missing capable () check in compat ioctl
tcp : fix potential memory corruption
i40e : Setting queue count to 1 using ethtool is valid
sched / migration : Move calc_load_migrate () into CPU_DYING
perf tools : Don ' t free list head in parse_events__free_terms
ARM : EXYNOS : Fix potential NULL pointer access in exynos_sys_powerdown_conf
mm : fix negative nr_isolated counts
[ PATCH ] kyrofb : Fix uninitialized value
ext4 : Fix overflow caused by missing cast in ext4_fallocate ()
staging : omapdrm / omap_gem_dmabuf . c : fix memory leakage
aha1542 : Convert aha1542_intr_reset to function
cifs : fix potential overflow in cifs_compose_mount_options
ath9k : do not register LEDs on AR913x
i2c : ocores : Fix pointer to integer cast warning
USB : ohci_usb can oops on shutdown
perf intel - pt : Fix off - by - one comparison on maximum code
USB : prevent autosuspend during hub initialization
[ MTD ] [ NAND ] Casting bug in nand_default_block_markbad
[ POWERPC ] kprobes : Fix possible system crash during out - of - line single - stepping
[ media ] media / vivid - osd : fix info leak in ioctl
iio : adc : meson - saradc : unlock on error in meson_sar_adc_lock ()
HID : emsff : properly handle emsff_init failure
partitions / msdos : FreeBSD UFS2 file systems are not recognized
isofs : avoid info leak on export
uwb : adds missing error handling
ext4 : fix memory leak in xattr
mfd : Mark two lm3533 zone registers as volatile
gpio : mb86s70 : Return error if requesting an already assigned gpio
Btrfs : Check for NULL page in extent_range_uptodate
x86 / fpu / xstate : Return NULL for disabled xstate component address
ceph : avoid divide by zero in __validate_layout ()
UBI : fix error path in __wl_get_peb
ARM : OMAP2 : Fix tusb6010 init error and compilation warning
gpio - 74x164 : Fix buffer allocation size
iwlwifi : don ' t ack the card state notification
dmaengine : zxdma : fix memset call
ath10k : fix memory leak
perf annotate browser : Exit when pressing ESC or the left arrow
ring - buffer : check for less than two in size allocation
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
sunrpc : don ' t register UDP port with rpcbind when version needs congestion control
Btrfs : send correct rdev and mode in btrfs - send
[ SCSI ] iscsi_tcp : update header size during relogin
max17042_battery : Handle irq request failure case
iio : chemical : ccs811 : Corrected firmware boot / application mode transition
hwmon : ( w83627hf ) Fix vrm write operation
staging : comedi : ni_660x : remove BUG_ON () in ni_660x_request_mite_channel ()
cxlflash : Fix to prevent stale AFU RRQ
dmaengine : at_hdmac : use tasklet_kill in teardown
[ media ] omap3isp : Fix buffer flags handling when querying buffer
[ SERIAL ] 8250_early . c passing 0 instead of NULL
tg3 : Disable 5717 serdes and B0 support
batman - adv : Add BATADV_DBG_TP_METER to BATADV_DBG_ALL
thermal : rockchip : optimize sensor auto accessing period
scsi : qla2xxx : correctly shift host byte
ipv6 : ignore null_entry in inet6_rtm_getroute () too
[ media ] cx231xx : prints error code if can ' t switch TV mode
rtlwifi : rtl8192se : Synchronize IRQ after disabling it
wifi : wilc1000 : validate length of IEEE80211_P2P_ATTR_CHANNEL_LIST attribute
[ media ] videobuf2 : fix sparse warning
offb : the pseudo_palette is only 16 elements long
[ S390 ] cio : Fix ungroup vs . machine check race in ccwgroup .
ext3 : fdatasync should skip metadata writeout when overwriting
drm / amdgpu : update rev id for hainan
xfs : check for a valid error_tag in errortag_add
ALSA : timer : Fix leak in events via snd_timer_user_ccallback
powerpc / xive : Fix IPI reset
bcma : support chipsets with PMU and GCI cores ( devices )
KVM : fix LAPIC timer period overflow
gpio : omap : add missed spin_unlock_irqrestore in omap_gpio_irq_type
jfs : prevent NULL deref in diFree
iscsi - target : fix extract_param to handle buffer length corner case
staging : lustre : check for integer overflow
[ SPARC64 ]: Allocate TSB node - local .
[ PATCH ] libata : fast exit from EH while unloading
ACPI : resolve HP nx6125 S3 immediate wakeup regression
ext2 : Check return value from ext2_get_group_desc ()
virtio : console : Make write () return - ENODEV on hot - unplug
xfs : fix acl count validation in xfs_acl_from_disk ()
usb - uas : set max_lun and max_channel
mmc : sdhci - acpi : Handle return value of platform_get_irq
mips : handle pgtable_page_ctor () fail
dlm : avoid unnecessary search in search_rsb
ASoC : blackfin : Fix missing break
mac80211 : fix statistics leak if dev_alloc_name () fails
usb : gadget : atmel_usba_udc : Remove unneeded condition
ixgbe : add check for VETO bit when configuring link for KR
xfrm_user : fix info leak in copy_to_user_state ()
console / vgacon . c : mark file - local symbol static
[ media ] bfin_capture : fix compiler warning
blkcg : don ' t create " io . stat " on the root cgroup
i7core_edac : don ' t free on success
hwmon : ( adm1025 ) Fix vrm write operation
blkcg : stop iteration early if root_rl is the only request list
tg3 : Restrict ASPM workaround devlist
Staging : comedi : off by one in doDevConfig ()
perf , x86 : Fix Intel fixed counters base initialization
tun : allow positive return values on dev_get_valid_name () call
module : fix memory leak on early load_module () failures
staging : mt7621 - dma : Fix ident by space
irqchip / gic - v3 : Fix out - of - bound access in gic_set_affinity
libbpf : fix string comparison for guessing eBPF program type
selinux : fix error return code in policydb_read ()
cgroup : add sparse annotation to cgroup_iter_start () and cgroup_iter_end ()
ARM : mvebu : mvebu - soc - id : add missing clk_put () call
uprobes : split THPs before trying to replace them
[ PATCH ] powerpc : IOMMU : don ' t ioremap null addresses
drm / i915 : don ' t pwrite tiled objects through the gtt
ASoC : wm5102 : Correct lookup of arizona struct in SYSCLK event
xfrm : fix possible null deref in xfrm_init_tempstate
drm / i915 : check the power domains in ironlake_get_pipe_config ()
irda : fix overly long udelay ()
[ ALSA ] sound / pci / cs46xx /: fix an off - by - one
crypto : qat - silence a static checker warning
PCI : cpqhp : register busn_res
tty : amba - pl011 : don ' t dereference NULL platform data
iommu / rockchip : Play nice in multi - platform builds
cpufreq_stats : do not remove sysfs files if frequency table is not present
i40e : Expose ' trust ' flag to userspace via ndo_get_vf_config .
Btrfs : copy string correctly in INO_LOOKUP ioctl
[ CIFS ] Possible null ptr deref in SMB2_tcon
iio : adc : xilinx - xadc : assign auxiliary channels address correctly
x86 : check range in reserve_early ()
dma : struct device - replace bus_id with dev_name (), dev_set_name ()
ipv6 : fix a typo in fib6_rule_lookup ()
af_key : initialize satype in key_notify_policy_flush ()
[ media ] marvell - ccic : fix V4L2_PIX_FMT_SBGGR8 support
kernel / exit . c : call proc_exit_connector () after exit_state is set
sched : Fix information leak in sys_sched_getattr ()
drivers / video / backlight / lcd . c : call put_device if device_register fails
block : fix use - after - free in seq file
[ TCP ]: Do receiver - side SWS avoidance for rcvbuf < MSS .
drm / exynos : add pid to g2d_runqueue_node
drm / i915 : Can ' t do accurate vblank timestamps with UMS
[ media ] drivers / media / pci / saa7134 / saa7134 - dvb . c : Test if videobuf_dvb_get_frontend return NULL
perf tools : Disallow PMU events intel_pt and intel_bts until there is support
perf ui hist browser : Fix segfault on ' a ' for annotate
liquidio : update module parameter fw_type to reflect firmware type loaded
bcache : fix use - after - free in btree_gc_coalesce ()
mnt : Fix the error check in __detach_mounts
[ NETFILTER ]: H . 323 helper : fix possible NULL - ptr dereference
e1000e : e1000e_cyclecounter_read (): fix er32 ( SYSTIML ) overflow check
iommu / vt - d : Remove warning for HPET scope type
vmwgfx : return an - EFAULT if copy_to_user () fails
hv_netvsc : use kcalloc
[ ALSA ] fix resource leak in usbmixer
KVM : x86 : emulator : Return to user - mode on L1 CPL = 0 emulation failure
lz4 : ensure length does not wrap
ixgb : convert dev -> priv to netdev_priv ( dev ).
ASoC : WM8903 : Free IRQ on device removal
i40evf : Add additional check for reset
dccp : fix info leak via getsockopt ( DCCP_SOCKOPT_CCID_TX_INFO )
spi : lpspi : fix memory leak in fsl_lpspi_probe
cxl : remove redundant increment of hwirq
clk : shmobile : Free ' clock ' on error path
chipidea : error on overflow for port_test_write
cs5535 - mfgpt : don ' t call __init function from __devinit
Btrfs : fix inode eviction infinite loop after extent_same ioctl
i40e : Fix a sleep - in - atomic bug
mfd : Use the AB8500 ' s IRQ domain to convert hwirq to virq
bpf : Fix integer overflow in prealloc_elems_and_freelist ()
f2fs : exclude special cases for f2fs_move_file_range
[ media ] vivid : fix broken Bayer text rendering
drm / nouveau / core : swap the order of imem / fb
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
ASoC : Intel : Skylake : Fix memory leak during init instance
phy : exynos - mipi - video : fix check on array index
ceph : avoid null deref in osd request error path
drm / i915 : fix EDID memory leak in SDVO
iwl : off by one bug
[ media ] v4l2 - mem2mem : clear m2m queue ready counter in v4l2_m2m_streamoff
acer - wmi : set the touchpad toggle key code to KEY_TOUCHPAD_TOGGLE
Staging : rtl8187se : Remove unnecessary braces around single statement in dot11d . c
Input : ALPS - fix command mode check
irda : Fix missing msg_namelen update in irda_recvmsg_dgram ()
mtd : mtd_blkdevs : don ' t increase ' open ' count on error path
xen / pci : Check for PCI bridge before using it .
compat : fix 4 - byte infoleak via uninitialized struct field
ath9k : fix channel frequency calculation for AR9340
iomap : prepare iomap_fiemap for attribute mappings
ARM : mxc : free dma_mask in error path
ptr_ring : fail early if queue occupies more than KMALLOC_MAX_SIZE
cpufreq : intel_pstate : Fix limits -> max_perf rounding error
Input : return correct size when reading modalias attribute
eCryptfs : Copy up lower inode attrs after setting lower xattr
mmc : atmel - mci : r / w proof capability only available since v2xx
i2c : core : smbus : fix a potential missing - check bug
kbuild : fixdep : Check fstat ( 2 ) return value
stmmac : re - add IFF_UNICAST_FLT for dwmac1000
x86 / msr : Add capabilities check
scsi : qla2xxx : Fix system crash for Notify ack timeout handling
KVM : x86 : Check lapic_in_kernel () before attempting to set a SynIC irq
arm64 : Move BP hardening to check_and_switch_context
dmaengine : fix dependency chaining
drm / vmwgfx : Don ' t flush fb if we ' re in the suspended state .
staging : r8723au : move constant to right of comparison test
USB : serial : io_ti : fix information leak in completion handler
iio : buffer : Fix NULL pointer deref caused by empty scan mask
imx - drm : ipu - dp : Check the return value of devm_kzalloc ()
sched : debug : update exec_clock only when SCHED_DEBUG
ALSA : emu10k1 - Fix digital / analog switch on audigy2 ZS
RDS : Heap OOB write in rds_message_alloc_sgs ()
ext4 : Avoid null pointer dereference when decoding EROFS w / o a journal
ASoC : intel : log an error on double free
genetlink : Fix a memory leak on error path
HID : elo : fix memory leak in elo_probe
mfd : db8500 - prcmu : Check return of devm_ioremap for error
sg_start_req (): make sure that there ' s not too many elements in iovec
[ media ] em28xx : Fix vidioc fmt vid cap v4l2 compliance
drm / amdkfd : Handle case of invalid queue type
ath6kl : unblock fwlog_block_read () on exit
ARM : ux500 : set coherent_dma_mask for dma40
sock_diag : off by one checks
xfs : fix directory hash ordering bug
mtd : mtdpart : fix add_mtd_partitions error path
audit : don ' t generate audit feature changed log when audit disabled
ethtool : time to blink provided in seconds not jiffies
staging : lustre : llog : prevent out - of - bound index
f2fs : handle error case with f2fs_bug_on
ASoC : omap - pcm : Free dma buffers in case of error .
af_key : initialize xfrm encap_oa
rtlwifi : btcoex : 21a 2ant : just return when wifi is under ips
mac80211 : fix scan_sdata assignment
gma500 : Consider CRTC initially active .
staging : ozwpan : Set farewell report length .
avr32 : Fix sigaltstack behaviour
KEYS : Fix keyring ref leak in join_session_keyring ()
drm / i915 : switch off backlight for backlight class 0 brightness
[ SCSI ] scsi_transport_iscsi : make priv_sess file writeable only by root
drm / helper : lock all around force mode restore
[ media ] uvcvideo : Fix race of open and suspend in error case
hpsa : Checking for a NULL return from a kzalloc call
[ NETFILTER ]: recent match : missing refcnt initialization
[ XFS ] Wait for all I / O on truncate to zero file size
dmaengine : pxa_dma : declare transfer are reusable
mmc : debugfs : Print ext_csd in ascending order
udp : harden copy_linear_skb ()
ixgbe : return error on unsupported SFP module when resetting
usb : chipidea : add system interface for ttctrl . ttha
cdrom : information leak in cdrom_ioctl_media_changed ()
drm : Fixup locking WARNINGs in drm_mode_config_reset
drm / amdgpu : it should disable gfxoff when system is going to suspend
drm / mediatek : plane : Use FB ' s format ' s cpp to compute x offset
batman - adv : Calculate extra tail size based on queued fragments
staging : comedi : ni_6527 : reset edge detection registers
bq20z75 : Enable detection after registering
drm / nouveau / pmu : make sure the reset hook exists before running it
serial : 8250 : of : Defer probe on missing IRQ
ipv6 : fix possible crashes in ip6_cork_release ()
ARM : mm : allow set_memory_ *() to be used on the vmalloc region
iwlwifi : protect SRAM debugfs
printk : use vprintk_func in vprintk ()
staging : dgnc : Use kcalloc instead of kzalloc .
iwlagn : block regular host commands if driver don ' t own uCode
ksmbd : fix memory leak in smb2_handle_negotiate
[ MTD ] CORE mtdchar . c : fix off - by - one error in lseek ()
rt2x00 : allow AP and mesh mode to operate simultaneously
kernel / params . c : fix the maximum length in param_get_string
ALSA : compress : fix an integer overflow check
pinctrl / nomadik : use zero as default irq_start
OMAPDSS : TPO - TD03MTEA1 : Check for errors from regulator_enable ()
ath5k : temporarily disable crypto for AP mode
OMAP : HWMOD : Adding clockdomain check
regmap : cache : Don ' t attempt to sync non - writeable registers
inetpeer : initialize -> redirect_genid in inet_getpeer ()
[ ALSA ] hda - codec : Add two new systems to ALC883
inotify : fix coalesce duplicate events into a single event in special case
s390 / dasd : fix list corruption for sleep_on requests
iio : accel : bmc150 - accel : fix counting direction
esas2r : fix an error path in esas2r_ioctl_handler
cifs : always do is_path_accessible check in cifs_mount
ALSA : seq : Fix race at timer setup and close
iwlwifi : mvm : Set valid TX antennas value before calib request
tracing / filters : free pred when clearing filters
drm : restore open_count if drm_setup fails
ipvs : fix info leak in getsockopt ( IP_VS_SO_GET_TIMEOUT )
clk : shmobile : r8a7795 : Add USB3 . 0 clocks
mm : avoid endless looping for oom killed tasks
mac80211_hwsim : fix possible memory leak in hwsim_new_radio_nl ()
x86 : add dummy acpi_numa_processor_affinity_init () implementation on 32 - bit
drm / i915 : Don ' t leave old junk in ilk active watermarks on readout
[ PATCH ] s390 : bug in setup_rt_frame
brcmfmac : Fix memory leak and missing assignment .
mutex : Fix missing conditions to build mutex_spin_on_owner ()
lockd : fix regression in lockd ' s handling of blocked locks
ipv4 : dont drop packet in defrag but consume it
mtd : nand : ifc : Initialize SRAM for all version >= 1 . 0
platform / x86 : dell - wmi : check for kmalloc () errors
cassini : fix off by one
drm / radeon : fix typo in finding PLL params
KEYS : close race between key lookup and freeing
mlxsw : spectrum_router : Compare only trees which are in use during tree get
mmc : mxcmmc : handle mmc_of_parse () errors during probe
KVM : avoid NULL pointer dereference in kvm_dirty_ring_push
mac80211 : Keep skb -> piority for relayed frames in AP mode
[ media ] sony - btf - mpx : Drop needless newline in param description
Staging : bcm : info leak in ioctl
mmc : Fix mmc_delay () function
f2fs : fix to return correct error number in f2fs_write_begin
atmel - mci : Fix memory leak in atmci_regs_show
powerpc / 64 : Don ' t use early_cpu_has_feature () in cpu_ready_for_interrupts ()
Staging : lustre : module : Replace function calls
drm / i915 : fix integer overflow in i915_gem_do_execbuffer ()
[ media ] cec : ignore messages when log_addr_mask == 0
tools : iio : iio_utils : check amount of matches
[ IA64 ] kexec fails on systems with blocks of uncached memory
mac80211 : fix channel type recalculation with HT and non - HT interfaces
powerpc / pseries : Optimize IOMMU setup
[ PATCH ] ipw2200 : mark " iwconfig retry 255 " as invalid
block : fix use - after - free in sys_ioprio_get ()
mtd : pcmciamtd : remove dead code
HID : hyperv : match wait_for_completion_timeout return type
jbd2 : fix use after free in kjournald2 ()
ASoC : img - parallel - out : Add pm_runtime_get / put to set_fmt callback
ALSA : hda - Enable widget power saving for Realtek codecs
lock debug : sit tight when we are already in a panic
make sure do_dentry_open () won ' t return positive as an error
ath9k : don ' t run periodic and nf calibation at the same time
bpf : force strict alignment checks for stack pointers
crypto : user - fix memory leak in crypto_reportstat
scsi : storvsc : fix SRB_STATUS_ABORTED handling
l2tp : fix info leak via getsockname ()
[ SCSI ] zfcp : Wait for port scan to complete when setting adapter online
powerpc : Add missing prototype for hdec_interrupt
block : Fix io_context leak after clone with CLONE_IO
dm crypt : missing kfree in ctr error path
[ PATCH ] ext2 : prevent div - by - zero on corrupted fs
mwifiex : fix leak of gen_ie storage on exit from mwifiex_del_mgmt_ies
sched / numa : Fix math underflow in task_tick_numa ()
ALSA : seq : Cancel pending autoload work at unbinding device
[ XFS ] Check return value of xfs_buf_get_noaddr ()
ovl : ignore permissions on underlying lookup
[ PATCH ] cpufreq : fix section mismatch warnings
ALSA : usb - audio : Fix NULL dereference in create_fixed_stream_quirk ()
ax25 : fix info leak via msg_name in ax25_recvmsg ()
dm log writes : move IO accounting earlier to fix error path
iwlwifi : Fix memory leak in iwl_req_fw_callback ()
drm / i915 : Return stored value from max freq sysfs entry
prlimit : do_prlimit needs to have a speculation check
staging : iio : compress return logic
ALSA : hdspm - Fix PCI ID for PCIe RME MADI cards
[ ALSA ] seq_midi_event : fix parsing of F9 / FD bytes
[ SCSI ] ibmvfc : Driver version 1 . 0 . 4
fs : Rearrange inode structure elements to avoid waste due to padding
Btrfs : add missing kfree in btrfs_destroy_workqueue
seq_buf : Fix overflow in seq_buf_putmem_hex ()
mm : numa : Do not account for a hinting fault if we raced
usb : gadget : fsl_qe_udc : Remove unneeded condition
drm / amd / display : Fix FMT truncation programming
staging / lustre / ofd : refill env in ofd_get_info
tproxy : allow non - local binds of IPv6 sockets if IP_TRANSPARENT is enabled
tracepoints : use rcu_ * _sched_notrace
dm kcopyd : avoid pointless job splitting
ath10k : ensure txrx - compl - task is stopped when cleaning htt - tx
drivers / video / igafb . c : introduce lost ' return '
[ MTD ] [ OneNAND ] fix call to onenand_verify when writing subpages
omap3 : beagle : Fix USB host port power control
[ media ] v4l : vb2 - memops : use vma slab when vma allocation
yam : fix a memory leak in yam_siocdevprivate ()
ALSA : timer : Fix missing queue indices reset at SNDRV_TIMER_IOCTL_SELECT
ARM : mxc : platform - mxc - mmc : Fix register region size
tilegx : Don ' t receive packets when the napi budget == 0
vfs : make gcc generate more obvious code for acl permission checking
drivers : net : xgene : fix possible use after free
[ media ] r820t : Fix IF scale
f2fs : turn down IO priority of discard from background
nvmet : fix space padding in serial number
scanner : don ' t crash on strings containing a NUL byte
add sanity check for reading multipart files with no parts (# 840 )
reduce B44 _tmpBufferSize ( was allocating two bytes per byte ) (# 843 )
Fixed possible NULL pointer dereference .
add assert for addition overflow on queue creation (# 225 )
Protected against garbage in the CapRTL encoder
Reset the output value on error in _gnutls_x509_dn_to_string ()
eliminated double - free in the parsing of dist points
libnsgif : fix oob in lzw_decode
fix heap - buffer - overflow in mp4read . c
lib / bidi : Fix integer overflow ( found by fuzzing )
driver : secure : use consttime_memequal for memory comparison
Fixed potential buffer overflow in ` ProcessRadioRxDone `
parse : Check for non - empty command / rspfile / rspfile_content
Fix a number - up crash bug ( Issue # 413 )
Handle NO response without message properly
fixed missing return status test error
Fixed typo while calculating module path length .
Fixed frame allocation from an awaited frame .
checkpath : fix allocation size of path buffer
[ sipmsgops ] fix codec_delete_XX () parsing ( 2 )
[ sipmsgops ] fix codec_delete_XX () parsing
KML : set OAMS_TRADITIONAL_GIS_ORDER for SRS returned on returned layers
Fixed Memory Overflow Errors
Decoder : Fixed error handling for dangling fields
Fixed error concealment when no MBs are decoded in the current pic
Decoder : Fix for handling invalid intra mode
Decoder : Initialize default reference buffers for all pictures
IGraphicBufferConsumer : fix ATTACH_BUFFER info leak
dm : validate inputs in vq_endchains
Fix a wrong check in rfc_parse_data
fixed segfault in GIF reading ; this fixes https :// github . com / pts / sam2p / issues / 67
GenericSource : reset mDrmManagerClient when mDataSource is cleared .
Resolve a merge issue between lmp and lmp - mr1 +
IOMX : do not clear buffer if it ' s allocated by component
modbus_reply : fix copy & paste error in sanity check ( fixes # 614 )
demux : mp4 : fix buffer overflow in parsing of string boxes .
misc : update : fix buffer overflow in updater
codec : schroedinger : fix potential buffer overflow .
Problem : ZMTP v1 static allocator is needlessly resized
avoid XSS vulnerability when using OIDCPreservePost On
fixed overflow vulnerability
fail HELLO command when SSL is required
resolve BUGFIX handle yet unresolved if - features
Prevent an OOB read in ` expand_dims . cc `
[ tflite ] Ensure ` MatchingDim ` does not allow buffer overflow .
Prevent array write out - of - bounds .
[ lite ] Add validation check for dilation height / width to be positive integers .
[ security ] Fix failed shape check in RaggedTensorToVariant .
Remove unused out - of - bounds memory access from MTM loader (# 249 )
* Fixed Github Vulnerability Report GHSL - 2022 - 066
[ tls ] Treat invalid block padding as zero length padding
transport / server . cc : Return correct size of decompressed lz4 buffer
remove assert that can reasonably be expected to happen
ensure fragment offset is less than total length
Fix the | name | of PerformanceNavigationTiming
Avoid an overflow in harfbuzz - myanmar . c
chrome_repack_locales : compress . pak files on Chrome OS
[ scheduler ] Remove implicit fallthrough in switch
gpu : Disallow use of IOSurfaces for half - float format with swiftshader .
Drive out additional flakiness of TouchAction browser test .
Fix a lock re - entry bug in GpuDataManager :: UpdateGpuInfo .
Removing unnecessary DCHECK from SafeBrowsing interstitial .
Hide the " Open PDF in Reader " bubble on navigations .
[ MediaStream ] Pass request ID parameters in the right order for OpenDevice ()
Disables PanelBrowserTest . MinimizeTwoPanelsWithoutTabbedWindow on
Prevent regular mode session startup pref type turning to default .
Add CHECKs for file descriptors used in select () by InotifyReaderTask
Handle a bad XSLT expression better .
Tests were marked as Flaky .
Remove an unused variable .
Use the lock when accessing the buffer object .
fix incorrect TexImage3D params w / UNPACK_IMAGE_HEIGHT
Upgrade a TextIterator ASSERT to a RELEASE_ASSERT as a defensive measure .
Fix a crash in HTMLFormElement :: prepareForSubmission .
rm flag and rebaseline
Take default system font size from PlatformFont
