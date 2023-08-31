ovs_events plugin : Replace strncpy () with sstrncpy ().
Fixed memory leak in collectdclient library .
Added the instance name to varnish API
fix leak in cc_mslsa . c
strdup ( 0 ) now returns 0
src : fix nullptr dereference for signal during startup
src : make CipherBase :: kind_ const
return with CL_EFORMAT instead of CL_EDSIG
fix previous commit
Fix a election calculation
decrease the repeating of the same log
wallet : Add error handling . Check return value of ParseUInt32 (...) in ParseHDKeypath (...).
repository : plug memory leak
checkout : free last created directory
refs : fix unused - but - set warning
fetch - pack : Finish negotation if remote replies " ACK % s ready "
git_connect_git (): forbid newlines in host and path
git - pickaxe : fix nth_line ()
credential : avoid writing values with newlines
[ PATCH ] Fix git - init - db creating crap directories .
imap search : Fixed YOUNGER / OLDER
lib - fs : fs_write_stream_finish ( file , NULL ) is now alias to fs_write_stream_finish_async ( file )
lib : hash - format - fix leak on _init () failure path
POP3 : A bit more code for handling nicely unsupported auth mechanisms .
doveadm - mail - mailbox : Whitespace fix
lib - index : Fixed sequence lookup of newly created mails in transaction view .
i_stream_stat () should return NULL for closed istreams
data stack : Fixes to handling out - of - memory situations .
lib - storage : Previous change incorrectly reverted setting parent shared namespace usable .
openssl : Fixed Curl_ossl_cert_status_request () not returning FALSE
imap : Added check for new internal imap response code
hostip . c : return immediately from Curl_resolv_timeout () upon expired timeout .
smb : Don ' t try to connect with empty credentials
clarified the URL part based on the problems Martin Hilpert had
Improved a line of code checked in earlier .
Fix a unit test mistake .
Fix a coverity complaint .
pua : release lock on not finding temp dialog
topoh : free nbuf
dmq : initialize peer_list to NULL by default
freeing filetype_str when not needed anymore .
ao / wasapi : more consistent / reliable method of computing extra WAVEFORMATEXTENSIBLE size
demux_lavf : mark ASS tracks as always UTF - 8
vo_opengl : free to - be - added hook on hook array overflow
msg : don ' t prefix slave - mode stuff by default
wayland : don ' t change set fullscreen twice
ffmjpeg yv12 422 fixed
vo : mark frame lost after seek reset
fix broken term_osd output in update_osd_msg ()
fpgautils : Check for fgets error
DevAPI : for_each_ { managed , logical } _proc helper macros
Bugfix : RPC : Avoid processor - specific details in devdetails
Fixed bug in AcpiGetHandle (): searching for a relative path always
Initialized Status , line 386 .
ACPICA : test timeout on global lock interface
acpi : acpica : fix acpi operand cache leak in nseval . c
Hardware : Fix possible recursive locking in hwregs . c
typo in comment
Added AML_NAMEPATH_OP to AmlDumpOperand case statement .
slirp : Fill TCP / IP header template after SYN reception
PPC : e500 : correct params -> ram_size with ram_size
block / vmdk : do not report file offset for compressed extents
XBZRLE cache size should not be larger than guest memory size
qemu / iovec : Don ' t assert if sbytes is zero
usb : sanity check setup_index + setup_len in post_load
virtio : avoid buffer overrun on incoming migration
target - arm : Fix TCG temporary leaks for scalar VMULL
pulse - audio : fix bug on updating rpos
i8259 : Do not update IRQ output after spurious pic_poll_read
vfio - pci : Fix MSI / X debug code
vmdk : Fix format specific information ( create type ) for streamOptimized
dconf : disable swizzor heuristic by default
bz10893 : fixes bug for hash calculations when using -- allmatch .
tailf : fix memmove () usage
sys - utils / ipcutils : be careful when call calloc () for uint64 nmembs
Fix a memleak in torrentmodel .
fix : segfault through python bindings with fd to non - package (# 24089 ).
added ignoreSet to rpmRunTransaction ()
added an error message when rmdir () fails
conversations : don ' t rename to self !
Oops , got the sense of a return backwards .
http_jmap . c : gracefully handle NULL mailboxes on close
cyr_expire : missing flag is fine
buf : don ' t realloc for every buf_ensure
cyr_expire : don ' t resurrect convdb for deleted user
mailbox : but if there ' s no partition , then it really is removed
Call clusterUpdateState () after CLUSTER SETSLOT too .
fix recursion typo in zmalloc_usable
lavc : set avctx -> hwaccel before init
remove unnecessary - 1 from snprintf len
avprobe : free options at the end of main ()
oops , potential overflow on really large blocks
rl2 : Avoid a division by zero
Check that the seekheads were created
force sample for qcelp when not stored in mov , fix # 968
wmapro : return early on unsupported condition
twinvq : validate that channels is not <= 0
mxfdec : return error if no segments are available in mxf_get_sorted_table_segments
fix a memleak in av_metadata_set ()
add a memory check just in case
fix HMAC error report
made paircompare () static
Added support for password - protected Jabber chatrooms .
Improved failure handling in jabber_si_handle_request (). If the imcb
Reset the captured - packet count when we start or restart a capture .
Avoid a possible dereference of null pointer .
mqtt : Copy UAT match_criteria value
Gryphon : do not use col_set_str with non constant strings
sprintf big fix
gtk : Fix null dereference in stats
Add a description for errno EINVAL ( Invalid Argument ) to
Extract the center frequency from Peek tagged files .
From : Susanne Edlund
Qt : Apply recent column widths when columns changed
Add a cast to try and fix the OSX buildbot .
Patch by Michael Wallner , signed off by Edin Kadribasic .
Fixed heap overflow
Fixed a memory leak inside imap_utf8 ()
Added check for small ZEMD_MM_SEG_SIZE
oops , 5 . 4 and 5 . 5 use malloc in TSRM
Fixed possible memory leak .
Fixed uninilized intern
libfreerdp - gdi : fix gdi_Glyph_Free () segfault
Reset the ThreadingMsgListModel after setting the previous model , too
switch - root : do not use close old_root_fd after rm_rf_children ()
install : don ' t choke on dead links
util : look at STDOUT , not STDIN , when determining tty columns
[ PATCH ] allow NAME_SIZE > SYSFS_PATH_MAX
swap : don ' t add too many deps for swap files form / proc / swap
libudev : fix sysnum logic for digit - only device names
path - lookup : use correct cleanup function for strv (# 5007 )
Check if header is there
catch error code
Fixed memory leak .
Fixed memory leak .
Fixed # 6009 : Bounds checks in autodetect_recv_bandwidth_measure_results
Fixed argument check in < tls_disconnect >
Fixed return value for DST .
Fixed windows command line
libfreerdp - rfx : encoder must increase frameIdx or mstsc would crash .
gdImageScaleTwoPass memory leak fix
Protect from missing CodeGen .
af_aresample : check ff_get_audio_buffer () failure
lavf / tee : fix leak of bsfs array .
avcodec / g2meet : Also clear tile dimensions on header_fail
avcodec / hevc_refs : Check nb_refs in add_candidate_ref ()
avformat / mov : fix integer overflow
bitstream : build_table , check table_nb_bits .
shorten : fix " off by padding " bug
mpeg4videodec : disable frame multithreading for GMC , its not implemented at all
cmdutils : use sws_freeContext () instead of av_freep ().
matroskadec : unset matroska -> done when seeking
flacdec : silence several " warning : X may be used uninitialized in this function "
ac3_fixed : fix out - of - bound read
avoid uninitialised variable warning and clarify code
avformat / smacker : check for format mismatch more completely
avcodec / utils : Fix overflow in get_bit_rates computations
h264 : silence warning about array index being out of bounds
avcodec / dcadec : Check that the added xch channel isnt already there
avcodec / utils : check the private context class
kill uninitialised variable warning in mp3_read_probe ()
aes : fix array index out of bounds warning
vmd : use the PALETTE_COUNT constant uniformly
dsicinav : Check for out of bounds reads
qdm2 : avoid integer overflow
avcodec / takdec : Fixes : integer overflow in AV_SAMPLE_FMT_U8P output
aviobuf : Handle a NULL buffer in avio_close_dyn_buf
jpeg2000 : Make nreslevel fields int
avcodec / alac : only set * got_frame_ptr when all channels have been decoded
opusdec : Don ' t run vector_fmul_scalar on zero length arrays
avcodec / truemotion2 : Fix integer overflows in tm2_high_chroma ()
vaapi_h264 : Scale log2_max_pic_order_cnt_lsb with max_b_frames
rawdec : use av_shrink_packet .
rv10 : consider B frames in low delay streams invalid .
flvdec : allocate large enough buffer so get_bits () doesnt overread
ituh263 : reject b - frame with pp_time = 0
examples / vaapi_encode : Fix leak on hwframe init failure
avcodec / jpeg2000 : Check that codsty -> log2_prec_widths / heights has been initialized
lavc / mediacodec_wrapper : fix potential jni global reference leak
audemux : Check channels isnt 0
lavfi : af_amerge options array was not ended with NULL
vc1dec : silence " may be used uninitialized in this function " warning from gcc
avcodec / jpeg2000dwt : Check ndeclevels before calling dwt_decode *()
videotoolbox : fix crash when decoding interlaced video with new API
mpc8 : Check the seek table size parsed from the bitstream
avcodec / rawdec : check for side data before checking its size
avcodec / interplayvideo : Check that enough data is available for opcode_0x9
avcodec / jpeglsdec : check shift for values that cause overflow later
vp56 : Check for missing reference frame data
vf_gradfun : make config_props work properly when called multiple times .
ffprobe : fix value_string () prefix printing for values with negative logarithm
avformat / cache : Avoid int - overflow in cache compare function
avcodec / pcm - dvd : reset last header on errors
Fix out of bound reads in the QDM2 decoder .
swscale : don ' t use planar output functions to write to NV12 / 21 .
ffplay : disallow seeking before the start of the file
lavfi : avfilter_merge_formats : handle case where inputs are same
avformat / asfdec_f : Assert that packet positions match in asf_read_pts ()
rtmpproto : Send chunk size on the network channel
flashsv : check deflateInit () return value
lavc / movtextdec : keep the min size instead of max to fix overread .
mkv : mark corrupted packets and return them
icodec : fix leaking pkt on error
lavc / ffjni : fix uninitialized variable
lzo : fix overflow checking in copy_backptr ()
bitpacked : fix potential overflow
avformat / mov : use 64bit for size in mov_skip_multiple_stsd ()
avformat / utils : avoid overflow in compute_chapters_end () with huge durations
smoothstreamingenc : fix integer overflow
parser : fix large overreads
rmenc : more than 2 streams are not currently possible .
avcodec / ass_split : Fix null pointer dereference in ff_ass_style_get ()
avcodec / mjpegdec : Check for rgb before flipping
libvo - amrwbenc : Add braces to shut up gcc warning .
tiffenc : fix bpp value for monow / monob formats
lavf / movenc : check ff_mov_init_hinting () return
update_frame_pool : use channel field
yadif : check input video size .
cmdutils : fix uninitialized variable ( type ) warning .
avcodec / opusdec : check alignment , misalignment could lead to crashes with avx
avcodec / mjpegdec : Check for pixfmtid 0x42111100 || 0x24111100 with more than 8 bits
matroskadec : add assert on lack of overflow in pkt_size + offset
aacsbr : Silence warning : max_qmf_subbands may be used uninitialized in this function
avcodec / alsdec : Clear MPEG4AudioConfig so that no use of uninitialized memory is possible
fixup_vorbis_headers : add missing malloc failure check
mpegvideo : Avoid 32 - bit wrapping of linesize multiplications
brstm : reject negative sample rate
xavs : Do not try to set the bitrate tolerance without a bitrate
checkasm : v210 : Fix array overwrite
avcodec / dcadsp : Fix runtime error : signed integer overflow
dpcm : Fix invalid writes
lavd / xv : fix memory leak
id3v2 : fix compiler warning for uninitialized variables
Correcting wrong looking stream_id validity check in avidec .
cdxl demux : do not create packets with uninitialized data at EOF .
mpegaudidec : 10l forgot buf_size
vfwcap : support MJPG compressed streams .
avutil / tree : fix memleaks
hevc_ps : make sure failing to decode an SPS always returns an error
avformat / dss : Use av_freep () to avoid leaving stale pointers in memory
avformat / avidec : Check nb_streams in read_gab2_sub ()
indeo4 : Skip null frames .
rtp : Fix integer underflow that could allow remote code execution .
Do a little more checking .
matroskadec : fix a sanity check .
aacsbr : fix integer overflow
avformat / pjsdec : dont increase pointer when its already at the end in read_ts ()
avformat / hlsenc : Fix a memory leak when http_persistent is 1
avcodec / hevc_filter : Fix invalid shift
avcodec / smvjpegdec : return the packet size instead of 0
avcodec / pngenc : Check that there is at least 1 frame
aacenc : fix out of array writes
mp3enc : fix signed C99 overflow
avformat / bethsoftvid : check 4th header byte in probe
avformat / mxfdec : cleanup if parsing the header metadata fails
lavc / ffjni : fix local reference leak
mpegvideo_enc : remove assert that has become obsolete with the new API
dump_stream_format : fix division by 0
lavfi / alphamerge : fix leak introduced in 217163eb
matroskadec : fix a memory leak
lavc : do not leak frames in avcodec_decode_audio4
lavc / libopenjpegenc : check av_frame_alloc () failure .
avcodec / wmalosslessdec : shrink output on error so no uninitialized data is returned
vaapi_h264 : Scale log2_max_pic_order_cnt_lsb with max_b_frames
lavf : don ' t leak corrupted packets
avcodec / mjpegdec : Check escape sequence validity
mss1 : improve check if decoded pivot is invalid
lavf / swf : Fix auto - detection of compressed files .
asfenc : fix leaking asf -> index_ptr on error
rawdec : propagate pict_type information to the output frame
segafilm : fix division by 0
swscale : fix signed overflow in yuv2mono_X_c_template
vp8 : avoid race condition on segment map .
avcodec / pgssubdec : Use av_fast_padded_malloc () for rle buffer
* fix double free
lavfi / fifo : add assert to ensure request was successfull .
avcodec / ulti : Check number of blocks at init
avcodec / pngdec : Clean up on av_frame_ref () failure
protect realloc overflow
asfenc : fix leaking asf -> index_ptr on error
mpc8 : check stream count before accessing stream 1 .
lavf / mpeg : Initialize a stack variable used by memcmp ().
mxfdec : Fix files > 2 GiB
mpegts : fix demuxing PES private stream 2
qtpalette : make the color_ * variables unsigned again
avformat / icodec : Fix crash probing fuzzed file
sanm : remove " duplicate " for loop .
libavformat / rtpenc_mpegts : check avformat_new_stream () return value
vaapi : return early from ff_vaapi_render_picture () without picture
matroskadec : fix leak on error
avcodec / mjpegdec : Fix DC overflow in decode_block ()
utils . c : fix crash with threading enabled .
avformat / libquvi : Fix whitelist handling
avformat / mov : fix avg_frame_rate calculation
rtsp : Fix compiler warning for uninitialized variable .
ffmpeg : Fix bsf corrupting merged side data
avcodec / takdec : Fix integer overflow in decode_lpc ()
rtsp : Don ' t use uninitialized data if there are no streams
avcodec / jpeg2000dec : Check SIZ dimensions to be within the supported range
avformat / flac_picture : allocate buffer padding for picture
libavformat / hdsenc : check init_file () return code
examples / filtering_audio : fix a memory leak .
libopencore - amr : check return value of avcodec_alloc_frame ()
avcodec / dvdsub_parser : never return 0 when the input isnt 0
Remove lowres support from wmv2 decoder
indeo4 : check ref_mb
opt : fix memleak on error of range_array
ffplay : add existence check on ic -> pb in decode_thread ()
avcodec / vp9_superframe_bsf : cache packets by creating new references rather than moving them
lavfi / deshake : fix crash in uninit ()
avformat / iff : check avio_read () return in get_metadata ()
avformat / smoothstreamingenc : Use av_mallocz_array ()
VP3 : Prevent stack corruption from an unset custom coding method .
avformat / ivfenc : fix division by zero
avcodec / flac_parser : Update nb_headers_buffered
avcodec / takdec : Fix runtime error : left shift of negative value - 360
avcodec / h264 / find_start_code : factorize addition out
avformat / dfa : check first frame offset in probe
libavcodec / x86 / h264dsp_mmx . c : add forgotten HAVE_YASM
crypto : Use av_freep instead of av_free
avcodec / apedec : use av_fast_padded_malloc ()
avcodec / dirac_vlc : Fix invalid shift
av_probe_input_format3 : support NULL as buffer . Fixes null ptr deref
wma : Clip WMA1 and WMA2 frame length to 11 bits .
lavc / options : fix leaks in avcodec_free_context
rtsp : Check for command strings without spaces
avcodec / ffv1dec : Clear quant_table_count if its invalid
tiff : set the correct return value when check_size () fails
wmavoice : fix a signed overflow
avformat / adxdec : check avctx -> channels for invalid values
idcin : fix memleak
Free the RTSPStreams in ff_rtsp_close_streams
lavd / v4l2 : do not fail when VIDIOC_ENUMSTD returns ENODATA
avcodec / ffv1dec : update progress in case of broken pointer chains
lagarith : check count before writing zeros .
ffmpeg_opt : Check for localtime () failure
remove useless cast that makes code unreadable
id3v2 : catch avio_read errors in check_tag
mov : fix heap buffer overflow
j2k : reorder multiplications to avoid interger overflows related to precincts
check std tag size before reading .
quickdraw : Make the palette opaque
avutil / fixed_dsp : Check for av_malloc () failure
avcodec / nvenc : Fix forcing constqp rc mode
avpacket : fix size check in packet_alloc
avformat / concatdec : avoid NULL dereference when failed to open file .
kvm - irqchip : only commit route when irqchip is used
qcow2 : Fix new L1 table size check ( CVE - 2014 - 0143 )
target / sh4 : fix TCG leak during gusa sequence
block : Fix anonymous BBs in blk_root_inactivate ()
fix : unrealize virtio device if we fail to hotplug it
Fix a crash with monitor input arriving before readline_start has been called
ppc / spapr_drc : fix memory leak
vl : new runstate transition : RUN_STATE_GUEST_PANICKED -> RUN_STATE_FINISH_MIGRATE
dma : Fix stupid typo / thinko
exec : Fix bounce buffer allocation in address_space_map ()
block / qcow2 - bitmap : fix use of uninitialized pointer
virtio : Plug memleak by freeing vdev
darwin - user : Remove unneeded null pointer check
macio : fix overflow in lba to offset conversion for ATAPI devices
vdi : Fix memory leak
arm / translate - a64 : mark path as unreachable to eliminate warning
virtio - net : don ' t touch virtqueue if vm is stopped
tcg : Fix off - by - one in assert in page_set_flags
qxl : Slot sanity check in qxl_phys2virt () is off by one , fix
block / qcow2 : Add missing GCC_FMT_ATTR to function report_unsupported ()
msix : implement pba write ( but read - only )
hw / display / exynos4210_fimd : Fix bit - swapping code
tcg / ppc64 : Fix zero extension code generation bug for ppc64 host
pcihp : fix possible array out of bounds
target - s390x : fix setcc in TCG mode
block : Fix bdrv_is_allocated () return value
iohandler : Use bool for boolean struct member and remove holes
vhost : remove an incorrect assert
qcow2 : Prevent numerical overflow
s390 / sclpconsole : prevent char layer callback during initialization
virtio - net : count VIRTIO_NET_F_MAC when calculating config_len
xilinx_timer : Fixed deadlock issue
thread - pool : add missing qemu_bh_cancel in completion function
vmxnet3 : validate interrupt indices read on migration
vnc : fix qemu crash when not configure vnc option
s390x : s390_cpu_get_phys_page_debug has to return - 1
ui / spice - display . c : add missing initialization for valgrind
rtl8139 : skip offload on short TCP header ( CVE - 2015 - 5165 )
block / parallels : fix access to not initialized memory in catalog_bitmap
scsi : Make device scsi - disk reject / dev / sg *
crypto : fix leak in ivgen essiv init
Bugfix : PowerPC 64 slbia never invalidates the first segment entry .
qcow2 : Fix leaks in dirty images
hw / 9pfs / virtio - 9p - posix - acl : Fix out - of - bounds access
target / arm : fix TCG temp leak in aarch64 rev16
scsi : initialise info object with appropriate size
vhost : assert the log was cleaned up
vnc : fix qemu crash because of SIGSEGV
ahci : fix buffer overrun on invalid state load
use new helper name
qcow2 : Fix error handling in qcow2_alloc_cluster_offset
slirp : check len against dhcp options array end
ARM : ACPI : fix the AML ID format for CPU devices
scsi - block : fix direction of BYTCHK test for VERIFY commands
kvm : Switch to unlocked MMIO
io : fix qio_channel_socket_accept err handling
xhci : fix numintrs sanity checks
QMP : Check " arguments " member ' s type
target - ppc / fpu_helper : Fix efscmp * instructions handling
block / vpc : fix uninitialised variable compiler warning
blkverify : Fix BDS leak in . bdrv_open error path
spice : fix memory leak
pcnet : fix rx buffer overflow ( CVE - 2015 - 7512 )
syscall : fix use of uninitialized values
cadence_uart : bounds check write offset
cadence_gem : avoid stack - writing buffer - overrun
qemu - char : fix warning ' res ' may be used uninitialized
iothread : check iothread -> ctx before aio_context_unref to avoid assertion
qdev : Free opts on failed do_device_add
vmdk : Fix use of uninitialised value
coroutine - sigaltstack : Change jmp_buf to sigjmp_buf
ivshmem : check shm isn ' t already initialized
check - qdict : Fix possible crash
tcg : correct 32 - bit tcg_gen_ld8s_i64 sign - extension
replay : bh scheduling fix
timer : stm32f2xx_timer : add check for prescaler value
qom : Fix memory leak in object_property_set_link ()
spapr_pci : parent the MSI memory region to the PHB
ppc405 : Fix memory leak
io : fix check for handshake completion in TLS test
sheepdog : fix NULL dereference in sd_create
hw / intc / exynos4210_combiner : Don ' t overrun output_irq array in init
vmxnet3 : validate queues configuration read on migration
vhost : fix a memory leak
cpus : initialize cpu -> memory_dispatch
ARM : fix segfault
qemu - img : add check for zero - length job len
kvm / ioapic : dump real object instead of a fake one
gdbstub : fix for commit 87f25c12bfeaaa0c41fb857713bbc7e8a9b757dc
xhci : fix overflow in usb_xhci_post_load
tests : fix rsp leak in postcopy - test
ivshmem - server : fix possible OVERRUN
qemu - config : fix leak in query - command - line - options
target - alpha : fix uninitialized variable
tests : fix memory leak in virtio - scsi - test
pci : Clean up error checking in pci_add_capability ()
vga : fix invalid read after free
vhost - user - bridge : fix resume regression ( since 2 . 9 )
usb : xhci : fix memory leak in usb_xhci_exit
linux - aio : Fix laio_submit error handling
hw / acpi / aml - build : Fix memory leak
vnc : Fix crash on ' info vnc ' after ' change vnc none '
qxl : call dpy_gfx_resize when entering vga mode
target - arm / translate - a64 : Fix wrong mmu_idx usage for LDT / STT
usb : xhci : no DMA on HC reset
qdev : Fix memory leak in function set_pci_devfn
usb : check USB configuration descriptor object
kvm : Mark VCPU state dirty on creation
block / mirror : check backing in bdrv_mirror_top_refresh_filename
qapi : fix memory leak in bdrv_image_info_specific_dump
qdev - properties . h : Explicitly set the default value for arraylen properties
hw / misc / mmio_interface : Return after error_setg () to avoid crash
s390 - ccw : Fix alignment for CCW1
qemu - char : fix infinite recursion connecting to monitor pty
hw / i386 / acpi - build . c : Fix memory leak in acpi_build_tables_cleanup ()
tests : fix ipmi - kcs - test leak
fsdev : Fix overrun after readlink () fills buffer completely
pci : Fix memory leak
block : Don ' t use error_abort in blk_new_open
qcow2 : Fix grow_refcount_table error handling
virtio - net : properly check the vhost status during status set
core / graph : free RGraphEdges when freeing long_edges list
Check if debug is dead in r_debug_kill function .
format / zimg : make sure there are enough bytes in the buffer
Disable self :// in sandbox mode
bail out when no proxy was configured
pid - 1 is not our child
logging : use fchmod instead of chmod to avoid TOCTOU race
plugins / python : fix tracebaker resource leaks on import error
Fixed a missing brake in a switch case construct , spotted by Crab .
Initialize all members .
added Jonatan Alam � as catalan translator
conf - parser : fix strv parsing
bus : fix issue with reference counting
util : cunescape - fix memleak in error path
hibernate - resume : refuse to run outside of an initrd
load - fragment : setting empty string to Nice = resets the previous assignments
libudev : monitor - export MAJOR / MINOR only if available
udev : properly detect reference to unexisting part of PROGRAM ' s result
cgroup : properly determine cgroups zombie processes belong to
pam : skip DBUS_SESSION_BUS_ADDRESS export when kdbus is not active
core / mount : skip incomplete mountinfo entries
resolved : use dns_answer_size () where appropriate to handle NULL DnsAnswer
don ' t choke if there are no sockets for a service
app : skip keyboard devices so the device list becomes reasonable
app : typo : make string translatable
[ ot ] Ensure layout in hb_ot_layout_table_get_lookup_count ()
[ t : 5131 ] closes # 5131 .
Fixed issue with retrying autocommitted transactions . We might need to
sco compile fix : MAXPATHLEN
refs # 5133 , fix compilation error
branches / zip : btr_cur_optimistic_insert (): If btr_page_reorganize ()
Fixed a bug in vis . c
Fixed bug in sub select after last merge
correct fix for the bug # 6151
branches / innodb +: row_purge_remove_sec_if_poss_low (): Remove a suspicious
r8682 @ Kushana : nickm | 2006 - 08 - 29 17 : 58 : 59 - 0400
take back half of my bugfix . comparing times sucks .
Bridges are always dirs
layouts : fix memory leak
removed debug message
[ profiler ] Use thread_exited profiler event for recording thread end events .
[ mono ] Fix maybe - uninitialized warning .
Check the packet size in dnspcap2protobuf
queue : win32 : fix symbol export for M_queue_exists ()
Fixed missing initialization
pathbar : don ' t unconditionally iterate over buttons when scrolling
window - slot : protect against no new view
fix memory leak in MPD Byte range objects
fixed # 1769 ( fuzz )
Fixed bug in image loader
Fixed out of bounds access in ASN . 1 Octet string
gio : changed default symbols
[ XrdCl ] Initialize pStateRedirec to avoid a segfault while recovering
groupserv : Actually free mygroups and groupacs .`
chanserv / antiflood : update mqueue_t . last_used when adding new messages
m_ssl_gnutls Only generate DH params when dh_params is inited
nickflood : Allow changes to UID
Convert the output limit back to the parameter
linux : Make modal dialog show more smoothly
ospf6d : don ' t run DR election early on " ipv6 ospf6 priority "
lib , stream : fix stream sanity checks
[ bgpd ] 64 - bit bugfix in community_del_val by Jeremy Jackson < jerj @ coplanar . net >
peephole optimization for LOADNIL before STRCAT ; ref # 3110
forgot to free irep -> reps
fix length check in read_section_debug (); fix # 1572
ikev2 : during Child SA rekey negotiation abort when ESP proposals mismatch .
Get openswan to send the remote host address to PAM during XAUTH
Prevent the LDAP validator from accepting an empty password .
cherokee_buffer_crc32 () did not handle empty buffers correctly .
xkbcomp : fix stack overflow when evaluating boolean negation
fixed input problem , partly :)
mmv : use xstrdup instead of strdup ( for allocation tracking ).
uv - unix : fix warning : control reaches end of non - void function
X3F / simple_decode_row : check for data offset limit
window - tracker : Fix memory leak
dns ( tcp ) - fix coverity cid 1374307
pop3_doing : don ' t call pop3_dophase_done () if already failed
http2 : Fix address sanitizer memcpy warning
Fixed possible NULL pointer exception in ssl_get_ciphersuite ()
diraliases : always set the tail of the list to NULL
Spit out definition of YY_CHAR early
cmd / gc : rephrase the invalid indexing operation error message
Unauthenticated DH client certificate fix .
X509_VERIFY_PARAM_free : Check param for NULL
Typo : should check mgf1md
Missing OPENSSL_free on error path .
fix small memory leak with string - type templates
core bugfix : small memory leak in external command parser
imudp : add error msg when listener wasn ' t created
Fixed bug in IRC plugin when receiving data from server
core : replace constant by sizeof in a call to snprintf
Fix a crash with xmllint -- path on empty results
fix : don ' t pass colour - codes to filters
Make a string translatable .
Refresh the log window on pool failure message at startup .
fix multiple authentication using S / Key w / privsep
fixed local DoS when UnregisterHandler was called for a not existing handler
runner : init tmp handler
Specialize the scrollbars for each platform
reset ` reprocess_is_too_early ` when reprocessing
add missing dd_close () to save_dump_dir_from_problem_data ()
bplist : Make sure to detect integer overflow when handling unicode node size
xplist : Prevent heap buffer overflow when parsing empty tags
define IN6_ARE_ADDR_EQUAL macro for WIN32
check if connection was accepted before logging
cbson : PySequence_GetItem returns a new ref
fix leak in handle_propfind ()
idevice : free ssl object when SSL handshake fails
BUG : porting bugfix from refactor @ 3da77df
tpm2_getmanufec : only attempt to persist the EK if a handle was provided
Fixing ' in ' opcode handler .
librados : take lock when signaling notify cond
osd : clear_prior when activating
mds : Keep journaler in readonly mode until replay completes .
rgw / rgw_rest . cc : fix 4K memory leak
Event : process event before pop it to avoid releasing
msg : MLogAck has a private destructor
rgw : was missing return value
osd : init my_stat
crush / CrushWrapper : fix out of bounds access
filestore : sync object_map on _set_replay_guard ()
add ceph_fsid_set helper
msgr : fix problem with forced stop of pipe
kclient : fix NULL dereferencing oops
tar : fix crash on broken tar file .
bgo # 748608 - Destroy cairo_t after use
remove invalid free
regcomp : use long jumps if there is any possibility of overflow
libvncserver : scale : cast to 64 bit before shifting
virDomainGetTime : Deny on RO connections
Throw on skipping an invalid type .
branches / zip : backport r3848 from 6 . 0 branch
Edited fix for # 1224 .
Fixed issue with ECDH - ECDSA cipher suites rejecting certificates without
initialize extra name entries to 0
[ ARM ] export copy_page
[ PATCH ] uml : activate_fd : return ENOMEM only when appropriate
drm / fb - helper : Do the ' max_conn_count ' zero check
staging : comedi : adv_pci_dio : do board reset early in (* auto_attach )
signal : always clear sa_restorer on execve
Input : ati_remote - add missing input_sync ()
omap : 4430SDP : Register the card detect GPIO properly
ext4 : let ext4_group_add_blocks () handle 0 blocks quickly
[ S390 ] cio : fix orb initialization in cio_start_key
igmp : Avoid zero delay when receiving odd mixture of IGMP queries
isicom : Deinline drop_dtr , save 112 bytes
iio staging : fix lis3l02dq , read error handling
ARM : OMAP : Initialize PWM backlight enable_gpio field
ext3 : update ctime when changing the file ' s permission by setfacl
cfg80211 : reset auth algorithm
[ ETH ]: Validate address in eth_mac_addr
intel - iommu : Fix reattaching of devices to identity mapping domain
Input : imx6ul_tsc - check for negative return value
drm / i915 : Fix intel_tile_width () parameters
davinci_emac : off by one
[ PATCH ] lockdep : annotate scheduler runqueue locks
[ XFRM ]: xfrm_address_t annotations
[ media ] si2157 : advertise Si2158 A20 firmware
[ PATCH ] struct path : convert openpromfs
[ SCSI ] More USB deadlock fixes
[ PATCH ] Mobil Pentium 4 HT and the NMI
KVM : i8259 : simplify pic_irq_request () calling sequence
[ IA64 ] Remove sparse warning from unwind code
[ SCSI ] be2iscsi : memset wrb for ring create
perf script : Fix a NULL pointer dereference
iio : iio : Fix iio_channel_read return if channel havn ' t info
ath9k_hw : index out of bounds
ieee802154 : 6lowpan : Drop PACKET_OTHERHOST skbs in 6lowpan
[ PATCH ] janitor : reiserfs : super . c - vfree () checking cleanups
taskstats : cgroupstats_user_cmd () may leak on error
fs : Fix theoretical division by 0 in super_cache_scan ().
[ SPARC64 ]: Fix pfn_pte () build failure .
perf inject : Do not repipe attributes to a perf . data file
bonding : always update last_arp_rx on packet recieve
[ MIPS ] Fix oprofile module unloading
sdhci : don ' t warn about sdhci 2 . 0 controllers
ath9k : Use memcpy in ath_clone_txbuf ()
[ media ] r820t : better report signal strength
um : finally kill -> init_str leaks
Fix an OOPS when initializing IR remote on saa7134
HID : core : fix reporting of raw events
NFSD : Put exports after nfsd4_layout_verify fail
cxgb4 : allow large buffer size to have page size
/ drivers / staging / dgrp : Fixed warning unchecked sscanf return value
drm / nouveau : fix crash with noaccel = 1
drivers / net / usb / usbnet . c : Use FIELD_SIZEOF macro in usbnet_init () function .
[ IA64 ] mbcs_init () should give up unless running on sn2
xen / xenbus : export xenbus_dev_changed
kconfig : initialize the screen before using curses ( 3 ) functions
[ NETFILTER ]: Remove okfn usage in ip_vs_core . c
drm / exynos : exynos_hdmi : Pass correct pointer to free_irq ()
kdb : break out of kdb_ll () when command is terminated
powerpc / vphn : NUMA node code expects big - endian
cpufreq : e_powersaver : call CPUFREQ_POSTCHANGE notfier in error cases
[ S390 ] cio : fix memleak in resume path
cciss : handle allocation failure
ALSA : hda / realtek - Create " Bass Speaker " for two speaker pins
macb : Fix speed setting
drivers / rtc / rtc - da9052 . c : register ability of alarm to wake device from suspend
ALSA : hda - Force to reset IEC958 status bits for AD codecs
[ SCSI ] gdth : fix buffer overflow
tracing / ftrace : make nop tracer reset previous entries
[ PATCH ] USB : check for device in zd1201_resume
wl18xx : create per - chip - family private storage
[ PATCH ] orinoco : Make nortel_pci_hw_init () static .
staging : ozwpan : Check for correct config number .
sys_sync (): fix 16 % performance regression in ffsb create_4k test
ARM : prevent VM_GROWSDOWN mmaps extending below FIRST_USER_ADDRESS
[ media ] pvrusb2 : Fix a few missing default control values , for cropping
drm / mgag200 : check alloc_apertures () success in mga_vram_init ()
KVM : ioapic : fix assignment of ioapic -> rtc_status . pending_eoi ( CVE - 2014 - 0155 )
usb : dwc3 : gadget : add a debugging print when initializing endpoints
ASoC : tlv320aic3x : Convert codec -> hw_read to snd_soc_read
[ SPARC64 ]: Fix loop termination in mark_kpte_bitmap ()
Btrfs : should add a permission check for setfacl
thermal : core : ignore invalid trip temperature
rtlwifi : rtl8192cu : Fix error in pointer arithmetic
perf machine : Fix leak of ' struct thread ' on error path
drm / nouveau / bios : check for null script pointers in parser
l2tp : fix a potential skb leak in l2tp_xmit_skb ()
[ PATCH ] inotify : fix idr_get_new_above usage
Btrfs : Fix a crash when mounting a subvolume
fealnx : Write outside array bounds
rtlwifi : rtl8821ae : Fix problem with regulatory information
ALSA : cs4232 : fix crash during chip PNP detection
x86 / boot - image : Don ' t leak phdrs in arch / x86 / boot / compressed / misc . c :: Parse_elf ()
suspend : switch the Asus Pundit P1 - AH2 to old ACPI sleep ordering
drm / i915 : Permit pinning whilst the device is ' suspended '
b43 : Force - wake queues on init
[ PATCH ] remove unnecessary blk_queue_bounce in SG_IO
netrom : fix info leak via msg_name in nr_recvmsg ()
staging , media , easycap : Fix mem leak in easycap_usb_probe ()
sigtimedwait : use freezable blocking call
ir - core : Fix the delete logic
[ TG3 ]: Update version to 3 . 85
drm / exynos / fbdev : set smem_len for fbdev
sonice : Fix build due to botched netdev_alloc_skb () conversion .
sh : Account for INITIAL_JIFFIES when using jiffies clocksource .
drivers / cdrom / gdrom . c : fix device number leak
[ PATCH ] scsi_error thread exits in TASK_INTERRUPTIBLE state .
mac80211 : fix PS vs . scan race
ARM : 8544 / 1 : set_memory_xx fixes
ata : fix sparse warning in ata_piix . c
usb : host : xhci : Fix Null pointer dereferencing with 71c731a for non - x86 systems
[ SCSI ] bnx2fc : Drop incoming ABTS
arm64 : KVM : use ' int ' instead of ' u32 ' for variable ' target ' in kvm_host . h .
e100 : make sure vlan support isn ' t advertised on old adapters
mx3fb : fix regression with uninitalized fb_info -> mm_lock mutex
xfrm6 : release dev before returning error
perf stat : better error message for unsupported events
rtc : Fix ioctl error path return
staging : et131x : don ' t cast a void * to a struct pointer
misc : genwqe : Fix potential memory leak when pinning memory
xfs : check if device support discard in xfs_ioc_trim ()
dmaengine : mv_xor : fix big endian operation in register mode
drivers / net / wan : introduce missing kfree
toshiba_acpi : Fix mis - merge
i2c : designware : retry transfer on transient failure
[ ALSA ] hda - codec - Add first generation macbook subsystem ID
drm / i915 / crt : Flush register prior to waiting for vblank .
crypto : pcrypt - Use crypto_aead_set_reqsize helper
tty : xuartps : Fix " may be used uninitialized " build warning
hub : debug message for failing to enable device
tracing / filters : free pred when clearing filters
decnet : fix shutdown parameter checking
staging : comedi : do not return - ENOSYS .
staging / zcache : Fix memory leak while dropping an o2net_sock_container object
[ POWERPC ] Make irq_dispose_mapping ( NO_IRQ ) a nop
mtd : nand : mxc_nand : Check the return value from clk_prepare_enable ()
gianfar : Fix missing sock reference when processing TX time stamps
Btrfs : fix race between allocate and release extent buffer .
ath9k : Remove unused rx_edma in ath_rx_addbuffer_edma ()
ALSA : hda - Add MSI blacklist
iio : core : Handle error when mask type is not separate
staging : mt29f_spinand : Export OF module alias information
uselib : add missing MNT_NOEXEC check
libata : initialize qc -> dma_dir to DMA_NONE
GFS2 : Fix a use - after - free that coverity spotted
perf symbols : Properly align symbol_conf . priv_size
[ media ] media : davinci : vpif_display : drop unused member fbuffers
[ S390 ] idle : avoid RCU usage in extended quiescent state
m68k : export atari_keyb_init
[ MTD ] mtd / ofpart . c : add MODULE_LICENSE
[ SCSI ] qla2xxx : Process DPC requests within valid Fabric topologies .
staging : wilc1000 : removes potential null dereference
parisc : isa - eeprom - Fix loff_t usage
w1 : fix for loop exit condition in w1_seq_show ()
firewire : core : remove an unnecessary zero initialization
Staging : comedi : fix memory leak in comedi_bond . c
md : check for memory allocation failure in faulty personality
r8169 : balance pci_map / pci_unmap pair
ALSA : hdsp - Ignore MIDI and PCM events in interrupts until initialized
[ XFS ] Check return value of xfs_buf_get_noaddr ()
i40e : validate VSI param from VFs
ACPI : Fix crash on ASUS laptops
ath10k : fix num_legacy_stations tracking
xfs : ensure we capture IO errors correctly
[ GFS2 ] Fix calculation of demote state
perf tools : callchain : Fix spurious ' perf report ' warnings : ignore empty callchains
[ PATCH ] ppc32 : don ' t call progress functions after boot
mlx4_core : Fix memory leak in mlx4_enable_msi_x ()
batman - adv : Remove space before semicolon
ARM : EXYNOS : Fix potential NULL pointer access in exynos_sys_powerdown_conf
[ PATCH ] audit : Moved variable declaration to beginning of function
fs / binfmt_misc . c : add terminating newline to / proc / sys / fs / binfmt_misc / status
[ media ] vb2 : Check if there are buffers before streamon
drm / radeon : fix UVD 256MB check
[ NETFILTER ]: nfnetlink_queue : fix computation of allocated size for netlink skb .
perf intel - pt : Fix off - by - one comparison on maximum code
dm : swap target postsuspend call and setting suspended flag
JFS : FIx one more plain integer as NULL pointer warning
mmap : check -> vm_ops before dereferencing
drm / sti : in crtc_atomic_flush , enable only planes of this crtc
iommu / vt - d : Fix NULL pointer dereference in device_to_iommu
AFS : Fix possible null pointer dereference in afs_alloc_server ()
greybus : core : fix hd - creation error path
USB : ftdi_sio : fix minor typo in get_ftdi_divisor
[ TCP ] tcp_probe : use GCC printf attribute
ath9k_hw : print device ID if not supported
drbd : fix memory leak in drbd_adm_resize
i40e : stop the service task at shutdown
mtd : atmel_nand : trivial : change DMA usage information trace
fb_defio : Push file_update_time () into fb_deferred_io_mkwrite ()
KVM : SVM : Set the ' busy ' flag of the TR selector
greybus : greybus_protocol : fix order of sdio get caps response
usb : xhci : Fix OOPS in xhci error handling code
usb : gadget : storage : release superspeed descriptors .
[ PATCH ] Add deprecated_for_modules
[ PATCH ] um : fix - up schedule_timeout () usage
s390 / io : add ioport_map stubs
pNFS : Skip invalid stateids when doing a bulk destroy
fsl / fman : fix error handling
Btrfs : fix clone ioctl where range is adjacent to extent
cpufreq : prevent lockup on reading scaling_available_frequencies
drm / radeon / kms : mmio base / size should be resource_size_t .
Staging : rc2860 : return - EFAULT on copy_to_user errors
drbd : merge_bvec_fn : properly remap bvm -> bi_bdev
skge : Unmask interrupts in case of spurious interrupts
Input : twl4030 - pwrbutton - fix a leak of the IRQ during init failure
fs : befs : remove unnecessary * befs_sb variable
radiotap : fix bitmap - end - finding buffer overrun
iommu / amd : Free domain id when free a domain of struct dma_ops_domain
video : s3c - fb : return an error when bpp is invalid
[ SPARC64 ]: Fix of_device bus_id settings .
f2fs : avoid to trigger writepage during POR
dm bufio : initialize read - only module parameters
rpc : fix garbage in printk in svc_tcp_accept ()
ALSA : snd - usb - caiaq : Lock on stream start / unpause
ocfs2 : Filter - ENOSPC in mlog_errno ()
ds2760_battery : Add missing kfree ( di ) in ds2760_battery_remove ()
ath6kl : add missing of_node_put
usb : gadget : net2280 : Fix NULL pointer dereference
i40e : Expose ' trust ' flag to userspace via ndo_get_vf_config .
[ IPV6 ]: Reorg struct ifmcaddr6 to save some bytes
Input : gpio - keys - report error when disabling unsupported key
[ libata ] ata_piix : add HP compaq laptop to short cable list
loop : fix NULL dereference if mount fails
block : check partition alignment
drm / radeon : don ' t share PPLLs on DCE4 . 1
rtnl : fix msg size calculation in if_nlmsg_size ()
staging : slicoss : use status in slic_ioctl
tcp : fastopen : avoid negative sk_forward_alloc
mm : vmscan : fix misused nr_reclaimed in shrink_mem_cgroup_zone ()
gpio : aspeed : Add open - source and open - drain support
mac80211 : mesh only parameter mppath maybe unused
batman - adv : Detect missing primaryif during tp_send as error
amd64_edac : fix ECC checking
[ S390 ] cio : dont forget to set last slot to NULL in ccw_uevent ().
staging : slicoss : fix a leak when kzalloc fail
drm / ttm : test for dma_address array allocation failure
mlxsw : spectrum : Don ' t take multiple references on a FID
[ SCSI ] pm8001 : bit set pm8001_ha -> flags
ath : do not update cycle counters with sleep mode
flex_array : fix the panic when calling flex_array_alloc () without __GFP_ZERO
drm / radeon : don ' t setup audio on asics that don ' t support it
[ PATCH ] USB : don ' t allocate dma pools for PIO HCDs
spi : fix spi_s3c24xx_gpio device handle lookup
[ PATCH ] manage - jbd - its - own - slab fix
gro : Fix bogus gso_size on the first fraglist entry
Btrfs : return error code in btrfs_check_trunc_cache_free_space ()
KVM : fix fault page leak
[ SPARC64 ]: Use alloc_pci_dev () in PCI bus probes .
e1000e : increase minimum frame size allowed
usb : chipidea : udc : zero - length packet is only needed for TX
mei : reset client connection state on timeout
dm stripe : fix potential for leak in stripe_ctr error path
tmpfs : truncate prealloc blocks past i_size
drivers / net / cxgb3 / cxgb3_main . c : prevent reading uninitialized stack memory
s2io : Don ' t receive packets when the napi budget == 0
[ X25 ]: Restore skb -> dev setting in x25_type_trans ().
drm / i915 : CR clock gating is recommend to be set on PineView
spi : spi - davinci : Fix variable type
ALSA : hda - Intialize more codec fields in snd_hda_codec_reset ()
iwlagn : overwrite EEPROM chain setting for 6250 devices
rt2800 : fix wrong TX power compensation
[ SCSI ] megaraid : Use the proper type to hold the irq number .
USB : make the autosuspend workqueue thread freezable
tty / serial / sirf : fix error propagation in sirfsoc_uart_probe ()
ARM : shmobile : mackerel : remove OCR masks , where regulators are used
ALSA : lx646es : Fix possible uninitialized variable reference
PCI : of : Add 64 - bit address recognition without LPAE support
x86 : fix broken flush_tlb_others_ipi ()
f_phonet : fix page offset of first received fragment
[ SCSI ] qla2xxx : Correct IOCB queueing mechanism for ISP54XX HBAs .
mac80211 : fix ieee80211_sta_process_chanswitch for 5 / 10 MHz channels
KVM : ARM : VGIC : Optimize the vGIC vgic_update_irq_pending function .
[ ATM ] firestream : Fix uninitialized var warning .
inetpeer : fix token initialization
KVM : MMU : make for_each_shadow_entry aware of largepages
[ media ] tda8290 : Fix a bug if no tuner is detected
mm , vmstat : Remove spurious WARN () during zoneinfo print
ext4 : Fix ext4_free_blocks () w / o a journal when files have indirect blocks
ARM : mach - imx : clk - pllv2 : Staticize clk_pllv2_ops
usbfs : Fix oops related to user namespace conversion .
[ CPUFREQ ] powernow - k8 : get drv data for correct CPU
unix / dgram : peek beyond 0 - sized skbs
perf hists browser : Add NULL pointer check to prevent crash
sysctl : Add neg_one as a standard constraint
tracing : fix ref count in splice pages
greybus : uart : Fix the memory leak in connection init
[ ALSA ] ca0106 - Add missing sysfs device assignment
f2fs : fix wrong condition for unlikely
dm snapshot : fix metadata corruption
USB : add a private - data pointer to struct usb_tt
KVM : Grab pic lock in kvm_pic_clear_isr_ack
drm / radeon / atom : workaround vbios bug in transmitter table on rs780
staging : comedi : das16 : use comedi_handle_events ()
microblaze : Ptrace notifying from signal code
iwlwifi : mvm : disable uAPSD due to bugs in the firmware
qed : Fix error return code in qed_resc_alloc ()
OMAP : DSS2 : Taal : Fix ESD check
dlm : make posix locks interruptible
ide : add missing rq . ref_count initialization to ide_diag_taskfile ()
KVM : x86 : Improve thread safety in pit
drm / i915 / skl : Don ' t skip mst encoders in skl_ddi_pll_select ()
ASoC : imx - es8328 : Register jacks at the card level
[ ARM ] Use kcalloc to allocate counter_config array rather than kmalloc
USB : belkin_sa : avoid divide by zero error
drm / nouveau : v3 . 0 pll limits tables have type <-> register mapping too
perf tools : Fix map_groups__clone to put cloned map
perf / hwbp : Fix cleanup in case of kzalloc failure
KVM : PPC : Initialize linears with zeros
drbd : fix spurious protocol error
mwifiex : block data traffic to tx paused receive address
[ IPV6 ] ROUTE : Allow searching subtree only .
w1 : fix for loop in w1_f29_remove_slave ()
Btrfs : disable snapshot aware defrag for now
drm / i915 : Prevent use - after - free of inherited framebuffer
cfg80211 : fix potential connection work crash
Staging : wlan - ng : wiphy_free () is not called in case wiphy_register () fails
clk : tegra : Register the proper number of resets
lsm : Use a compressed IPv6 string format in audit events
sched : terminate newidle balancing once at least one task has moved over
Staging : bcm : info leak in ioctl
cxgb4vf : Use defined Mailbox Timeout
r8169 : fix a bug in rtl8169_init_phy ()
cdc_ncm : return - ENOMEM if kzalloc fails
crypto : img - hash - Fix hash request context
atm : ambassador : use NULL instead of 0 for pointer
[ PATCH ] v9fs : return the correct error when interrupted by signal
mtd : nand : tmio_nand : show parent device in sysfs
md : Fix raid1 -> raid0 takeover
tracing : Don ' t make assumptions about length of string on task rename
fbdev : bf537 - lq035 : add missing blacklight properties type
tools lib traceevent : Free filter tokens in process_filter ()
tun : update file current position
tracing / function - graph : Use correct string size for snprintf
OMAP : DSS2 : Check if display supports update mode changes
tty : serial : Use GFP_ATOMIC instead of GFP_KERNEL in serial8250_em485_init ()
drm / i915 : Fix NULL pointer deference when out of PLLs in IVB
arm : mxs : add irq_chip - name for GPIO IRQs
mfd : Fix possible s5m null pointer dereference
[ media ] xc5000 : use after free in release ()
p54 : use ether_addr_equal_64bits
[ PATCH ] invalidate_inode_pages2_range () debug
HID : extend autodetect to handle I2C sensors as well
staging : ozwpan : Check error condition before creating endpoint .
ALSA : maestro3 : Clear interrupts before enabling them
fuse : Initialize total_len in fuse_retrieve ()
stmmac : fix oversized frame reception
pm8001 : Fix to remove null pointer checks that could never happen
firewire : core : fix an information leak
sparc64 : Fix overshoot in nid_range ().
rtlwifi : rtl8192cu : Fix unitialized struct
iwlagn : use vif -> type to check station
rxrpc : Validate the net address given to rxrpc_kernel_begin_call ()
xfs : prevent leaking uninitialized stack memory in FSGEOMETRY_V1
[ media ] r820t : Fix IF scale
drm / radeon / kms : Don ' t kzalloc memory which is immediately overwritten .
mwifiex : Fix possible buffer overflows in mwifiex_ret_wmm_get_status ()
ASoC : Intel : Skylake : Don ' t unload module when in use
firestream : fix spelling mistake : " reseverd " -> " reserved "
wlcore : fix unsafe dereference of the wlvif
airo : return from set_wep_key () when key length is zero
Input : tegra - kbc - fix inverted reset logic
ASoC : SSM2602 : Add entry for the ssm2603 to the device id table
[ SCSI ] qla2xxx : Correct report - id acquisition check
ASoC : SOF : ipc : Fix memory leak in sof_set_get_large_ctrl_data
crypto : caam - fix output sequence contiguity check
e1000e : static analysis tools complain of a possible null ptr p dereference
ath5k : use tasklet_hi_schedule for beacon queue
fm10k : Fix a potential NULL pointer dereference
batman - adv : fix potential orig_node reference leak
perf hists browser : Fix segfault if use symbol filter in cmdline
tracing : Have error path in predicate_parse () free its allocated memory
cfg80211 : ignore invalid BSSIDs when looking for BSSes
usb : chipidea : usb2 : check memory allocation failure
ASoC : Free the card ' s DAPM context
[ media ] sh_veu : v4l2_dev wasn ' t set
[ PATCH ] net / pcnet32 : replace schedule_timeout () with msleep_interruptible ()
[ ALSA ] ASoC : Fix TLV320AIC3X mono line output interconnect
ALSA : timer : Fix leak in events via snd_timer_user_tinterrupt
drm / vmwgfx : Fix a buffer object leak
dmaengine : bcm - sba - raid : serialize dma_cookie_complete () using reqs_lock
rpmsg : char : release allocated memory
iwlwifi : pcie : fix TVQM queue ID range check
hv_netvsc : fix a race between netvsc_send () and netvsc_init_buf ()
Yama : handle 32 - bit userspace prctl
crypto : api - Fix crypto_drop_spawn crash on blank spawns
watchdog : Fix null pointer dereference while accessing rdc321x platform_data
misc : apds9802als : Fix the logic checking timeout in als_wait_for_data_ready ()
aio : mark AIO pseudo - fs noexec
[ ARM ] dma : convert IOMD DMA to use sg_next ()
ACPICA : Fix memory leak on unusual memory leak
staging : greybus : uart . c : change ' unsigned ' to ' unsigned int '
ext4 : set h_journal if there is a failure starting a reserved handle
irqchip / mips - gic : Map to VPs using HW VPNum
hugetlb : fix race condition in hugetlb_fault ()
Page migration : Do not accept invalid nodes in the target nodeset
drm / i915 : Annotate user relocs with __user
clk : axi - clkgen : Remove sometimes impossible check
md : raid1 : kill warning on powerpc_pseries
clk : hisilicon : hi3660 ： Fix potential NULL dereference in hi3660_stub_clk_probe ()
drm / tilcdc : Take CRTC lock when calling tilcdc_crtc_disable ()
io_uring / msg_ring : check file type before putting
tipc : check nl sock before parsing nested attributes
ipv4 : rate limit updating of next hop exceptions with same pmtu
[ S390 ] cio : Log the response from the unit check handler
f2fs : correct removexattr behavior for null valued extended attribute
[ SCSI ] be2iscsi : fix memory leak on error path
soc / tegra : pmc : Guard against uninitialised PMC clock
wl12xx : fix a memory leak of probereq template upon recovery
block : cciss : fix information leak to userland
KEYS : prevent KEYCTL_READ on negative key
vfio - pci : Add missing break to enable VFIO_PCI_ERR_IRQ_INDEX
[ PATCH ] sil24 : add testing for PCI fault
staging : rtl8188eu : use eth_broadcast_addr () in place of memset
orangefs : avoid freeing a slot twice in wait_for_direct_io ()
wl3501_cs : remove pedantic build warning
ptp_kvm : probe for kvm guest availability
[ PATCH ] powerpc : Fix bug in bug fix for bug in lmb_alloc ()
[ PATCH ] ppc64 : iSeries early printk breakage
qlcnic : Remove __QLCNIC_DEV_UP bit check to read TX queues statistics .
sctp : not allow transport timeout value less than HZ / 5 for hb_timer
ocfs2 : fix memory leak in dlm_migrate_request_handler ()
mac80211 : lock sdata in ieee80211_csa_connection_drop_work ()
[ PATCH ] mac80211 : fix GCC warning on 64bit platforms
sh : fix copy_from_user ()
stmmac : don ' t return zero on failure path in stmmac_pci_probe ()
igb : Fix a memory leak in igb_probe
sctp : support sysctl to allow users to use stream interleave
tipc : remove unnecessary call to dev_net ()
ASoC : imx - pcm - dma : check kzalloc return value in function snd_imx_open
staging : ozwpan : Check error condition before creating endpoint .
ARM : pxa : Improve a size determination in pxa3xx_u2d_probe ()
mwifiex : pcie : Fix memory leak in mwifiex_pcie_alloc_cmdrsp_buf
ath9k_hw : fix an off - by - one error in the PDADC boundaries calculation
ixgbe : DCB : remove DCB check config
staging : comedi : ni_labpc : fix a > 80 char line issue
iommu / io - pgtable - arm - v7s : Check for leaf entry before dereferencing it
mmc : dw_mmc : fix the wrong condition checking
af_iucv : exploit target message class support of IUCV
qed : Add missing stat for new isles
Staging : rtl8192u : Fix no spaces around '+'
drm / i915 : Add fifo underrun reporting state to debugfs
[ PATCH ] lockdep : annotate scheduler runqueue locks
xfs : use -> t_dfops in log recovery intent processing
[ PATCH ] stack overflow safe kdump : safe_smp_processor_id (): voyager
mqueue : fix a use - after - free in sys_mq_notify ()
iommu / amd : Check if domain is NULL in get_domain () and return - EBUSY
drivers / net / eql . c : prevent reading uninitialized stack memory
tipc : validate length of sockaddr in connect () for dgram / rdm
cpufreq : Do not clear real_cpus mask on policy init
xfs : fix missing error check in xfs_rtfree_range
greybus : interface : fix potential attribute - buffer overflow
drm / i915 : Fix out - of - bounds array access in bdw_load_gamma_lut
coresight : etb10 : Move etb_disable_hw () outside of lock
iommu : smar : Fix return value check of create_irq ()
staging : ozwpan : prevent overflow in oz_cdev_write ()
drm / atomic : Make atomic helper track newly assigned planes correctly , v2 .
sched : reorder struct sched_rt_entity to remove padding on 64 bit builds
cifs : fix use - after - free caused by invalid pointer ` hostname `
fm10k : don ' t try to stop queues if we ' ve lost hw_addr
staging : ozwpan : oz_pd_free () can be static
mwifiex : sdio : fix use after free issue for save_adapter
USB : sierra : fix urb and memory leak in resume error path
drm / amd / display : Do DC mode - change check when adding CRTCs
Btrfs : fix return value from btrfs_listxattr when buffer size is too small
drm / nouveau / fifo / gp104 : initial support
sh : Fixed the TMU0 reload value on resume
mac80211 : fix the assignment of mesh element TTL
KVM : Improve create VCPU parameter ( CVE - 2013 - 4587 )
usb : gadget : fsl : fix the fault issue on rmmod
ASoC : sst_platform : free the resources on fail path
tpm_infineon : remove use of __devexit_p
ima : fix add LSM rule bug
[ CIFS ] fix trailing whitespace
Btrfs : fix oops when btrfs_inherit_iflags called with a NULL dir
sysctl : Add neg_one as a standard constraint
UBIFS : return EROFS in case of broken commit
xen : events : do not free legacy IRQs
dccp : fix freeing skb too early for IPV6_RECVPKTINFO
sysfs : fix hardlink count on device_move
sched : Guarantee new group - entities always have weight
NVMe : End sync requests immediately on failure
f2fs : check range before defragment
ACPI : silence kmemcheck false positive
spi : imx : Gracefully handle NULL master -> cs_gpios
bnx2x : Add known PHY type check
HID : sensor - hub : fix potential memory leak
[ SCSI ] megaraid_sas : remove sysfs dbg_lvl world writeable permissions
spi : gpio : prevent memory leak in spi_gpio_probe
ALSA : HDA : Fix Oops caused by dereference NULL pointer
unix : Use FIELD_SIZEOF () in af_unix_init ().
rt2x00 : Don ' t count retries as failure
ALSA : usb - audio - clear chip -> probing on error exit
perf / core : Update filters only on executable mmap
proc : fix null pointer deref in proc_pid_permission ()
ALSA : als4000 : enable burst mode
[ SCSI ] sr : remove dead code
ASoC : Intel : Fix incorrect sizeof () in sst_hsw_stream_get_volume ()
xfrm : Fix kernel panic when flush and dump SPD entries
i2c : mux : reg : wrong condition checked for of_address_to_resource return value
video : mmp : fix memcpy wrong size for mmp_addr issue
staging : wilc1000 : add check for kmalloc allocation failure .
[ SCSI ] libsas : Warn if ATA device detected but CONFIG_SCSI_SAS_ATA not set
sock_diag : Fix out - of - bounds access to sock_diag_handlers []
CacheFiles : Fix incorrect test for in - memory object collision
drm / radeon : fix UVD 256MB check
OMAP3 : SRAM size fix for HS / EMU devices
xen - gntalloc : integer overflow in gntalloc_ioctl_alloc ()
drm / vc4 : Return - EINVAL on the overflow checks failing .
hpfs : fix warnings when the filesystem fills up
iptunnel : make rx / tx bytes counters consistent
btrfs : send : silence an integer overflow warning
ARM : 6665 / 1 : Add Snapper9260 rtc wakeup interrupt pin
[ SCSI ] be2iscsi : memset wrb for ring create
Input : twl6040 - vibra - do not reparent to grandparent
Btrfs : Add error handle for btrfs_search_slot () in btrfs_read_chunk_tree ()
parisc / PCI : Set type for LBA bus_num resource
scsi : lpfc : Fix NULL pointer dereference during PCI error recovery
xen / blkfront : Make sure that the device is fully ready before allowing release .
[ SCSI ] qla4xxx : a small loop fix
Btrfs : Fix balance panic
md : make recovery started by do_md_run () visible via sync_action
x86 / topology : Handle CPUID bogosity gracefully
typhoon : Add missing firmware copy .
mmc : sdhci : fix low memory corruption
usb : storage : return on error to avoid a null pointer dereference
vmscan : promote shared file mapped pages
USB : yurex : Fix missing URB_NO_TRANSFER_DMA_MAP flag in urb
Btrfs : avoid warning bomb of btrfs_invalidate_inodes
drm : bochs : Prevent double - free of fb helper
staging : most : fix sparse warning Using plain integer as NULL pointer
ipv4 : fix debug info in tnode_new
[ PATCH ] i2c : chips / ds1374 . c fixup
mtd : atmel_nand : increase chip_delay
signal : Fix name of SIGEMT in # if defined () check
tpm_crb : check for bad response size
ipv6 : fix devconf after adding force_tllao option
ibmvnic : Fix NAPI structures memory leak
[ ALSA ] aoa : platform function gpio : ignore errors from functions that don ' t exist
perf / core : Fix potential double - fetch bug
perf intel - PT / BTS : Add missing initialization
amd_iommu : do not dereference a NULL pointer address .
iio : inkern : allocate zeroed memory
iio : imu : adis16400 : release allocated memory on failure
hwmon : ( scmi ) handle absence of few types of sensors
[ PATCH ] Fix oops in vfs_quotaon_mount ()
EDAC : Respect operational state in edac_pci . c
x86 / irq : Fix move_irq_desc () for nodes without ram
xprtrdma : Fix DMA - API - DEBUG warning by checking dma_map result
f2fs : fix a dead loop in f2fs_fiemap ()
staging : rtl8188eu : Use kcalloc instead of kzalloc .
[ media ] tvp5150 : allow get / set_fmt on the video source pad
[ ALSA ] soc - Clean up tabs
soc : ti : knav : Add a NULL pointer check for kdev in knav_pool_create
ibmvnic : Free tx / rx scrq pointer array when releasing sub - crqs
iio : buffer : Free active scan mask in iio_disable_all_buffers ()
perf report : Compare symbol name for inlined frames when sorting
scsi : hpsa : do not reset enclosures
xfrm_user : fix info leak in copy_to_user_tmpl ()
drivers : net : slip : fix NPD bug in sl_tx_timeout ()
vhost : actually track log eventfd file
mtd : spear_smi : add NULL check on devm_kzalloc () return value
lib / decompress : set the compressor name to NULL on error
drm / i915 / execlists : Add a GEM_TRACE to show when the context is completed
drm / i915 / sdvo : Cleanup connector on error path
KVM : MTRR : remove MSR 0x2f8
netrom : Fix use - after - free caused by accept on already connected socket
mac80211 : fix register_hw error path
bcache : fix cache_set_flush () NULL pointer dereference on OOM
rapidio : switch to get_user_pages_fast ()
staging : fbtft : make const array gamma_par_mask static
ASoC : rockchip : Enable 192khz in hw_params operation
ALSA : usb - audio : Fix out - of - bound error
jiffies : add utility function to calculate delta in ms
virtio : fix memory leak in virtqueue_add ()
drivers : thermal : parent virtual hwmon with thermal zone
drm / radeon : do not hard reset GPU while freezing on r600 / r700 family
mlxsw : spectrum : Fix ageing time value
TTY : hvcs : fix missing unlock on error in hvcs_initialize ()
regmap : Silence warning on invalid zero length read
s390 / qeth : fix thinko in IPv4 multicast address tracking
ALSA : hda - Cirrus : Correctly clear line_out_pins when moving to speaker
x86 - 64 : Fix the failure case in copy_user_handle_tail ()
iio : iio_buffer_register : Skip channels with negative scan index
[ SCSI ] scsi_lib : use correct DMA device in __scsi_alloc_queue
mac80211 : fix a possible leak of station stats
ipv6 : udp : fix the wrong headroom check
perf tools : Don ' t free list head in parse_events__free_terms
USB : prevent autosuspend during hub initialization
Btrfs : fix em leak in find_first_block_group
isdn : off by one in connect_res ()
cxlflash : Fix to prevent stale AFU RRQ
scsi : qla2xxx : correctly shift host byte
sctp : Use correct sideffect command in duplicate cookie handling
Drivers : hv : fcopy : restore correct transfer length
ext2 : Check return value from ext2_get_group_desc ()
mmc : sdhci - acpi : Handle return value of platform_get_irq
xfrm_user : fix info leak in copy_to_user_state ()
perf , x86 : Fix Intel fixed counters base initialization
[ PATCH ] powerpc : IOMMU : don ' t ioremap null addresses
irda : fix overly long udelay ()
x86 : check range in reserve_early ()
[ media ] marvell - ccic : fix V4L2_PIX_FMT_SBGGR8 support
sched : Fix information leak in sys_sched_getattr ()
video : vt8500lcdfb : Remove kfree call since devm_kzalloc () is used
mfd : Use the AB8500 ' s IRQ domain to convert hwirq to virq
f2fs : exclude special cases for f2fs_move_file_range
ipv4 : fib : Export free_fib_info ()
ceph : avoid null deref in osd request error path
drm / i915 : fix EDID memory leak in SDVO
Staging : rtl8187se : Remove unnecessary braces around single statement in dot11d . c
mtd : mtd_blkdevs : don ' t increase ' open ' count on error path
compat : fix 4 - byte infoleak via uninitialized struct field
iio : buffer : Fix NULL pointer deref caused by empty scan mask
imx - drm : ipu - dp : Check the return value of devm_kzalloc ()
kconfig : Avoid buffer underrun in choice input
ext4 : Avoid null pointer dereference when decoding EROFS w / o a journal
[ media ] em28xx : Fix vidioc fmt vid cap v4l2 compliance
aacraid : prevent invalid pointer dereference
xfs : fix directory hash ordering bug
rtlwifi : btcoex : 21a 2ant : just return when wifi is under ips
avr32 : Fix sigaltstack behaviour
mmc : debugfs : Print ext_csd in ascending order
KVM : VMX : fix page leak in hardware_setup ()
udp : harden copy_linear_skb ()
usb : chipidea : add system interface for ttctrl . ttha
drm : Fixup locking WARNINGs in drm_mode_config_reset
drm / mediatek : plane : Use FB ' s format ' s cpp to compute x offset
ipv6 : fix possible crashes in ip6_cork_release ()
ALSA : compress : fix an integer overflow check
inotify : fix coalesce duplicate events into a single event in special case
iio : accel : bmc150 - accel : fix counting direction
[ SCSI ] bnx2i : Added reconnect fix connecting against Lefthand targets
[ PATCH ] s390 : bug in setup_rt_frame
lockd : fix regression in lockd ' s handling of blocked locks
ipv4 : dont drop packet in defrag but consume it
KEYS : close race between key lookup and freeing
ARM : at91 / gpio : fix display of number of irq setuped
Staging : bcm : info leak in ioctl
drm / i915 : fix integer overflow in i915_gem_do_execbuffer ()
mac80211 : fix channel type recalculation with HT and non - HT interfaces
mtd : pcmciamtd : remove dead code
[ SCSI ] zfcp : Wait for port scan to complete when setting adapter online
drm / amd / display : Fix FMT truncation programming
tcp : initialize rcv_mss to TCP_MIN_MSS instead of 0
cred : copy_process () should clear child -> replacement_session_keyring
f2fs : turn down IO priority of discard from background
libnsgif : Fix null pointer deref on frameless GIF input
zero the arep structure ( Coverity # 115822 )
Fixed potential buffer overflow in ` ProcessRadioRxDone `
Fix a number - up crash bug ( Issue # 413 )
KML : set OAMS_TRADITIONAL_GIS_ORDER for SRS returned on returned layers
Fixed Memory Overflow Errors
IGraphicBufferProducer : fix QUEUE_BUFFER info leak
IOMX : do not clear buffer if it ' s allocated by component
modbus_reply : fix copy & paste error in sanity check ( fixes # 614 )
avoid XSS vulnerability when using OIDCPreservePost On
fixed overflow vulnerability
Prevent array write out - of - bounds .
[ tfg ][ functiondef_import ] Emit error on empty function attributes
ksmbd : check nt_len to be at least CIFS_ENCPWD_SIZE in ksmbd_decode_ntlmssp_auth_blob
chrome_repack_locales : compress . pak files on Chrome OS
[ MediaStream ] Pass request ID parameters in the right order for OpenDevice ()
Check the content setting type is valid .
Take default system font size from PlatformFont