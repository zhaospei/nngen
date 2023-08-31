daemon : ldm : avoid manual free ()
rrdtool plugin : Wait for the ` queue thread ' to exit ..
utils_cache plugin : Fix memory leak .
don ' t forget to liberate etype
buffer : guard against integer overflow
crypto : fix memory leak in ECDH :: SetPrivateKey
openssl : fix uninitialized memory access
Put our CIB into slave mode on exit
PE : Prevent a potential use of NULL
Fix a election calculation
Low : tools : crm_simulate - fix small xpath memory leak in inject_node_state ()
TE : Always exit via mainloop
oid : handle NULL pointers passed to git_oid_shorten_free ()
fileops : fix invalid read
credential : avoid writing values with newlines
git - show : Reject native ref
wt - status . c : fix a memleak
Illumos 5311 - traverse_dnode may report success when it should not
imap : Mark DELETE command as potentially closing mailbox .
doveadm proxy : Don ' t crash if remote doesn ' t support log proxying
mail_modifylog_mark_synced () didn ' t update mmap
doveadm - mail - mailbox : Whitespace fix
lib : Add asserts to t_abspath and t_abspath_to
lib - storage : mailbox_get_guid () crashed if storage didn ' t support GUIDs .
lib - auth : Fix memory leak in auth_client_request_abort ()
lib - index : mail - index - transaction - export : Fixed handling of transaction_flag_updates_have_non_internal () return value .
doveadm - save : Set exit code to EX_TEMPFAIL on open error
lib - storage : mailbox_list_delete_trash () - Don ' t ignore errors
authd : remove some whitespace
sasl : Fixed a memory leak on OOM
setopt : fix CURLOPT_SSH_AUTH_TYPES option read
Freed up an allocated pointer prior to a return .
introduced a new # include in response to Andrei ' s uri changes
core : free SE header parsed structure
pua : release lock on not finding temp dialog
ctl : free allocated pointer in case of error
tm : define tm_ctx_get () to NULL if no WITH_TM_CTX
Fixed two mem - leaks in Path - handling .
erlang : Fix memory leak
sca : fix snprintf buffer size for Expires header in replies to SUBSCRIBE .
Added some errorchecking in rs_set_warmth_auto ().
msg : don ' t prefix slave - mode stuff by default
demux_mkv : fix minor memory leak
fix scrolling status line in windows
don ' t call video -> get_info if it ' s NULL
fpgautils : Check for fgets error
Bugfix : RPC : Avoid processor - specific details in devdetails
Bugfix : Free stratum nonce1 before replacing it with new value on reconnect
Bugfix : Free shares discarded before beginning submission
Fixed bug in AcpiGetHandle (): searching for a relative path always
Initialized Status , line 386 .
Hardware : Fix possible recursive locking in hwregs . c
target - arm : fix unsigned 64 bit right shifts .
vl : Add failure check for SetEvent
fix full frame updates for VNC clients
tcg / optimize : fix setcond2 optimization
s390 / kvm : Add check for priviledged SCLP handler
block : keep bitmap if incremental backup job is cancelled
pulse - audio : fix bug on updating rpos
block / iscsi : fix uninitialized variable
target - mips : remove POOL48A from the microMIPS decoding
handle SD CMD5 without error messages
bb # 11135 - sigtool now verifies cvd prior to unpacking
bb # 11129 - applied unencoded '@' patch
fix loop in cab file processing : ensure window position within frame
libfdisk : gpt : avoid unnecessary pmbr check
sys - utils / ipcutils : be careful when call calloc () for uint64 nmembs
rpmdbRemove () should free header it read
sync : don ' t count EXPUNGED record annotations in CRC
log quirky searches
mboxlist : fix uninitialised memory use where pattern is " Other Users "
cyr_expire : missing flag is fine
cyr_expire : don ' t resurrect convdb for deleted user
mailbox : but if there ' s no partition , then it really is removed
http_dav : allow read - free - busy to be set via DAV
adding check error code
shorten : set invalid channels count to 0
avplay : clear pkt_temp when pkt is freed .
fixing segfault with http :// sam . zoy . org / zzuf / lol - ffplay . avi
put_bits : fix invalid shift by 32 in flush_put_bits ()
gcc bug workaround by ( Jonathan Gray < khalek at linuxgamers dot net >)
vsrc_movie : fix leak in request_frame ()
mpegts : Make sure we don ' t return uninitialized packets
dash : Use correct ISO C scanf conversion specifier
libopenh264enc : Return a more sensible error code in some init failure paths
Fix a read past end of buffer crash in the mp3 probe
lavfi : use designated initializers in avfilter_class
twinvq : validate that channels is not <= 0
smoothstreamingenc : Check the output UrlContext before accessing it
audemux : Add a sanity check for the number of channels
fix HMAC error report
purple : Fix crash on ft requests from unknown contacts
wsutil : return false if NULL is asked as profile name ( CID : 1405202 ).
[ SUA ] Change the dissector table sua . prop . tags to use DISSECTOR_TABLE_ALLOW_DUPLICATE
tshark : fix double memory free when analyzing several RTP streams
Qt : Fix leaked interfaces array in Wireless frame
Put volatile back - without it , the compiler warns you .
Break out of an infinite loop . Fixes bug 95 .
Patch by Michael Wallner , signed off by Edin Kadribasic .
Fixed NULL pointer dereferencing
fix : prevent possible crash
IE we love you all ...
oops , 5 . 4 and 5 . 5 use malloc in TSRM
Reset the ThreadingMsgListModel after setting the previous model , too
[ PATCH ] extend exec_program []
resolved : merge two bools into a bitfield
systemctl : return diffrent error code if service exist or not (# 3385 )
sd - event : fix theoretical leak on error in event_make_signal_data
libudev : always add UDEV_LOG
udev : link - config - fix naming policy
journal : make sure to initially populate the space info cache (# 4807 )
rtnl : fix for 32bits
resolved : don ' t allow adding of ANY class / type RRs to local zones
sysctl : parse all keys in a config file
Drop the DH group check to a weaker version of the checks
Fix an off by 8
allocate cleam memory for cipher to play with
rdpei : Fix possible NULL ptr access
libfreerdp - core : fix server crash in case rdp key file is not provided .
Fixed return value for DST .
Fixed windows command line
fix php 72494 , invalid color index not handled , can lead to crash
Remove CINT remnant
Added missing default initializer for fDirector
avformat / asfdec_f : Correct skip to key code
mpeg4 : report frame decoding completion at ff_MPV_frame_end ().
dxva : DXVA2_ModeHEVC_VLD_Main10 does not support Main
lavf / tee : fix leak of bsfs array .
h264 : reset first_field when current_picture_ptr is reset
avcodec / aacdec_fixed : Fix undefined shift
smc : Fix overread .
tests / audiomatch : Fix compile warning
avutil / opt : dont crash on av_opt_set_dict () with NULL
lavfi / hqdn3d : avoid use of uninitialized variable .
avcodec / hevc : Silence " warning : ref0 / 1 may be used uninitialized in this function "
mpc8 : fix channel checks
libavformat / mxfdec : fix MXFPackage -> comment_refs memory leak
mxfdec : fix memleak on mxf_read_close ()
avformat / mpegenc : Check for av_mallocz () failure
ffprobe : fix crash in case - of is specified with an empty string
avcodec / wavpack : Fix runtime error : left shift of negative value - 1
matroskadec : unset matroska -> done when seeking
avcodec / g726 : Fix runtime error : left shift of negative value - 2
oggdec : Fix memory leak in setting up vorbis headers
avcodec / movtextdec : Call mov_text_cleanup () on close
avcodec / mdec : Fix runtime error : left shift of negative value - 127
hlsenc : Allocate enough space for the pattern string
sgi : check maximum supported resolution
wavpack : check ch_offset
pthread : don ' t increment index on zero - sized packets .
avcodec / avpacket : Check metadata key in av_packet_unpack_dictionary ()
avformat / smacker : check for format mismatch more completely
avcodec / wmv2enc : allocate padding for extradata and check malloc failure
threads : Perform the generic progress cleanup more carefully .
fix incorrect check for abs_diff_pic_num overflow
avformat / dump : Fix context / level for payload dump
lavc / avpacket : check for malloc failure
avcodec / alac : only set * got_frame_ptr when all channels have been decoded
lavfi / scale : allocate interlaced scalers only if needed .
mpegaudiodec : Validate that the number of channels fits at the given offset
rv10 : consider B frames in low delay streams invalid .
lavf / pcm : check size , do not produce invalid packets
libstagefright : avoid memory leak
mpevideo_enc : disallow multiple slices for h261 and flv
flashsv2_prime : check block before using it .
avcodec / extract_extradata_bsf : Fix leak discovered via fuzzing
avcodec / hevc : use av_mallocz () for allocating tab_ipm
avcodec / jpeg2000dwt : Check ndeclevels before calling dwt_decode *()
avcodec / mjpegdec : zero gb to silence warning about it being possibly uninitialized
Fix a warning on an uninitialized variable .
aacdec : Fix null pointer crash
msrle : Use FFABS to determine the frame size in msrle_decode_pal4
avcodec / dca_lbr : Fix off by 1 error in freq check
avformat / utils : avformat_find_stream_info fix a crash in case of oom
lavc / hevcdec : fix invalid use of ff_get_format ()
avutil / frame : fix av_frame_copy for unknown layouts
avcodec / webp : Update canvas size in vp8_lossy_decode_frame () as in vp8_lossless_decode_frame ()
h265_parse : skip zero sized NAL units
ffv1 : set internal frame defaults
wavpack : check K , fix assertion failure
h264 : Check that the codec isn ' t null before accessing it
truemotion1 : use av_freep ()
ffprobe : fix NULL pointer dereference in writer_close ()
smoothstreamingenc : Fix a memory leak on errors
wmalosslessdec : Reset residues to 0 if they were not decoded from bitstream .
libx265 : Pass through user - set frame type
h264 : initialize H264Context . avctx in init_thread_copy
msmpeg4 : ignore negative DC overflow
avutil / imgutils : correctly check for negative SAR components
dds : disable palette flag for compressed dds
rtpdec : Fix the minimum packet length for RTCP SR packets
Check for out of bound writes in the avs demuxer .
avformat / pcmdec : fix memory leak
ffserver : free pathname before bailing out
avcodec / mpeg4videodec : Clear interlaced_dct for studio profile
Set start_time to 0 for raw audio files .
avcodec / smc : Check remaining input
img2dec : dont set start_time / duration to invalid values
avformat / mxfdec : fix typo in error check of mxf_edit_unit_absolute_offset ()
avutil / mem_internal : add missing header includes
avcodec / wavpack : Fix integer overflow in wv_unpack_stereo ()
avcodec / mjpegdec : Check for rgb before flipping
avformat / mxfdec : fix null pointer dereference
dcadec : check xch_base_channel against channel_order_tab .
cmdutils : fix null pointer dereference
oggenc : fix " oggstream may be used uninitialized in this function " warning
hevc : fix wpp threading deadlock .
tiffenc : fix bpp value for monow / monob formats
avcodec / eatgv : use av_mallocz () for frame_buffer
ffprobe : fix uninitialized variable warning
avformat / cache : Use int64_t to avoid int overflow in cache_read
decode : Initialize ret before using it
ra144enc : avoid integer overflows .
cmdutils : fix uninitialized variable ( type ) warning .
xan : Prevent NULL dereference with missing palette
avcodec / nvenc : set correct error code
avformat / segment : Pass flags to child context
avidec : Check that the header chunks fit in the available filesize .
avformat / gxf : Use 64bit for res to avoid overflow
avcodec / alsdec : Clear MPEG4AudioConfig so that no use of uninitialized memory is possible
mpeg12dec : reset data size after parsing extradata .
avcodec / crystalhd : allocate padding for extradata
brstm : reject negative sample rate
vc1dec : fix block_off
vqf : check samplerate , avoid division by 0 .
checkasm : v210 : Fix array overwrite
ffprobe : free shared writer context on exit
Correcting wrong looking stream_id validity check in avidec .
avformat / apngdec : Use 64bit ret to avoid overflow
lavc / avpacket : Fix undefined behaviour , do not pass a null pointer to memcpy ().
avcodec_decode_audio : do not trust the channel layout , use the channel count .
electronicarts : check timebase , fix assertion failure
fixing q > 0 . 0 assert failure caused by overflow of variance for b frames
avformat / matroskadec : fix integer overflow
Fixed a segfault in the DCA decoder with corrupted streams .
avcodec / a64multienc : use av_frame_ref instead of copying the frame
mem : Handle av_reallocp (..., 0 ) properly
swr / rematrix : fix C99 left shift overflow
h264 : correct ref count check and limit , fix out of array accesses .
checkasm : v210 : Fix array overwrite
indeo4 : update AVCodecContext width / height on size change
lavfi : fix use - after - free in ff_filter_frame .
avcodec / hevc_filter : Fix invalid shift
mp3enc : fix signed C99 overflow
avcodec / ffv1dec : Fix out of array read in slice counting
avformat / bethsoftvid : check 4th header byte in probe
examples / demuxing : free AVPacket after usage
avcodec / msrle : check return code for success before use
avformat / mxfdec : cleanup if parsing the header metadata fails
Prevent calling init_vlc () with invalid parameters in motionpixels decoder .
Fix a mem leak in vc1_decode_frame ().
mxf_set_audio_pts : fix division by 0
avcodec / zmbv : Check that the buffer is large enough for mvec
lavf / ftp : fix possible crash
avcodec / snowdec : Fix runtime error : left shift of negative value - 1
lavc : do not leak frames in avcodec_decode_audio4
avcodec / mpeg4videodec : Clear mcsel before decoding an image
lavfi / lut3d : avoid overread .
af_aresample : fix leak on alloc failure
lavf : don ' t leak corrupted packets
vf_scale : don ' t leak SWS context .
avcodec / dirac_vlc : Fix invalid shift in ff_dirac_golomb_read_32bit ()
xwma : check bytes_per_sample , fix division by 0 .
asfenc : fix leaking asf -> index_ptr on error
avplay : reset rdft when closing stream .
avcodec / mpeg_er : Clear mcsel in mpeg_er_decode_mb ()
hlsproto : Properly close avio buffer in case of error
prores : Evaluate all the quantizers
swscale : fix signed overflow in yuv2mono_X_c_template
cafdec : fix overflow checking in read_header ()
avformat / thp : check av_get_packet () for failure
avconv : free packet in write_frame () when discarding due to frame number limit
rtmpproto : Lengthen the filename buffer when receiving streams
ipmovie : do not read audio packets before the codec is known
avcodec / mpegaudiodec_template : Correct return code on id3 tag discarding
avcodec / mjpegdec : Fix DC overflow in decode_block ()
lavf probe : prevent codec probe with no data at all seen
diracdec : Test mctmp and mcscratch for malloc failure .
avcodec / mpegvideo_enc : check that max_b_frames is not negative
rtsp : Fix a crash with the RTSP muxer
avformat / flac_picture : allocate buffer padding for picture
libopencore - amr : check return value of avcodec_alloc_frame ()
matroskadec : use uint64_t instead of int for index_scale
riff : fix division by zero for G726
avformat / ipmovie : Check OPCODE_CREATE_TIMER size
avutil / opt : initialize ret
lavfi / nnedi : Fix a memleak .
r3d : check that sampling rate is non negative .
avcodec / pcm : Fix overflow in bitrate computation
avcodec / imgconvert : fix possible null pointer dereference
avcodec / vqavideo : use av_mallocz () for decode_buffer ()
mov_probe : fix integer overflows
yuv2rgb : fix integer overflow in fill_gv_table ()
avcodec / mlpdec : Do not leave a invalid num_primitive_matrices in the context
avformat / movenc : Fix potential leak of sgpd_entries array .
decode : avoid leaks on failure in ff_get_buffer ()
audio_frame_queue : fix null ptr dereference
avformat / avio : fix memory leak in url_find_protocol
avcodec / bitstream : assert that no integer overflow happened when writing codes in build_table ()
avcodec / takdec : Fix runtime error : left shift of negative value - 360
avformat / hlsenc : Free context after hls_append_segment
ffmpeg : Fix crash with ost -> last_frame allocation failure
fix decoding of coupling leak info for normal AC - 3
sanity check whether dimensions are non - null
avs : fix infinite loop on end - of - stream .
libfdk - aacdec : Correct buffer_size parameter
cbs_h264 : Fix memory leak in error case
utils : fix avcodec_flush_buffers pre - reference counting compatibility
avcodec / mpeg4video_parser : Check for extradata parsing failure
avformat / utils : Do not select audio streams with unknown sample rate in av_find_best_stream ()
avcodec / mpeg4videodec : use av_fast_padded_malloc () for bitstream buffer
avconv : check that the output format context exists before accessing it
mpegts : Make sure we don ' t return uninitialized packets
smc : fix the bounds check
vf_overlay : ensure the overlay frame does not get leaked .
avcodec / vp56 : Reset have_undamaged_frame on resolution changes
movenc : fixes a questionable valgrind uninitialized value warning
avcodec / webp : Add missing input padding
vc1 : properly zero coded_block [] edges on new slice entry .
avcodec / wavpackenc : fix uninitialized ret
avcodec / pictordec : Check plane value before doing value / mask computations
mpegmux : add stuffing to avoid incomplete PCM frames
avcodec / aac_adtstoasc_bsf : Fix memory leak
qsvdec : Fix memory leak on error
avformat / asfdec_f : Fix memleak
avutil / fixed_dsp : Check for av_malloc () failure
doc / examples / transcoding : check encoder before using it
avcodec / exr : fix undefined shift in pxr24_uncompress ()
careful with mmap off
add LOG entry when opening feed in browser
kvm - irqchip : only commit route when irqchip is used
qcow2 : Assert against currently impossible overflow
qapi : Fix memory leak
target - sparc : on UA2005 don ' t deliver Interrupt_level_n IRQs in hypervisor mode
input : add sanity check
block : Plug memory leak on brv_open_image () error path
tests : test - qmp - commands : Fix double free
e1000e : Fix build with gcc 4 . 6 . 3 and ust tracing
block / iscsi : fix uninitialized variable
esp : cancel current request only if some request is in flight
block : Fix memory leak in hmp_drive_add_node ()
qemu - img : mandate argument to ' qemu - img check -- repair '
vnc : fix a memory leak in threaded vnc server
init_paths : fix minor memory leak
spapr_vscsi : fix build error introduced by f19661c8
block : fix qcow2_co_flush deadlock
arm / translate - a64 : mark path as unreachable to eliminate warning
ide : Correct improper smart self test counter reset in ide core .
tcg : Fix off - by - one in assert in page_set_flags
replay : Fix dangling location bug in replay_configure ()
char : don ' t silently skip tn3270 protocol init when TLS is enabled
ppc : Remove a potential overflow in muldiv64 ()
block : record new size in bdrv_dirty_bitmap_truncate
hw / i386 / acpi_build . c : Avoid shifting left into sign bit
target / loongarch : Clean up tlb when cpu reset
vfio - pci : Fix missing unparent of dynamically allocated MemoryRegion
xilinx_timer : Fixed deadlock issue
target - ppc : Bug in BookE wait instruction
fix segfault in setting migration speed
coroutine : Fix use after free with qemu_coroutine_yield ()
tcg : allocate s -> op_dead_iargs dynamically
hw / nvram / spapr_nvram : Device can not be created by the users
rtl8139 : skip offload on short TCP header ( CVE - 2015 - 5165 )
hw / sd . c : Set ILLEGAL_COMMAND for ACMDs in invalid state
cpus . c : qemu_mutex_lock_iothread fix race condition at cpu thread init
cuda : fix crash on Windows
audio : release capture buffers
qom : Make object_property_set_qobject ()' s input visitor strict
acpi - build : disable with - no - acpi
qiov : prevent double free or use - after - free
throttle : Assert that bkt -> max is valid in throttle_compute_wait ()
linux - user - i386 : Fix crash on cpuid
i386 : kvmvapic : initialise imm32 variable
char : serial : check divider value against baud base
hw / isa / pc87312 : Mark the device with user_creatable = false
target - i386 : cpu : Fix potential buffer overrun in get_register_name_32 ()
tcg / mips : Fix regression caused by typo ( copy + paste bug )
ehci : add sanity check for maxframes
target - s390x : fix CC computation for EX instruction
slirp : check len against dhcp options array end
powerpc pci : fixed packing of ranges []
hw / misc / ivshmem : Fix ivshmem_recv_msg () to also work on big endian systems
scsi : esp : check buffer length before reading scsi command
tcg / optimize : optimize " op r , a , a => mov r , a "
vl : free err
hw / arm / virt : fix cpu object reference leak
block : check for existing device IDs in external_snapshot_prepare ()
scsi - disk : release AioContext in unaligned WRITE SAME case
replay : fix error message
block - verify : fix 32 - bit build
acpi : Assert sts array limit on AcpiCpuHotplug_add ()
syscall : fix use of uninitialized values
vhost - user : print original request on error
dump : fix memory_mapping_filter leak
lm32_uart : fix receive buffering
acpi - build : fix memory leak with bridge hp off
qdev : Fix crash when using non - device class name on - global
tests : fix check - qom - interface leaks
vhost - user : verify that number of queues is non - zero
block : Fix multiwrite error handling
loader : fix undefined behavior in rom_order_compare ()
linux - user / syscall . c : remove warning : ‘ array ’ may be used uninitialized in this function
coroutine - sigaltstack : Change jmp_buf to sigjmp_buf
qemu - img : Fix leakage of options on error
hw / pci / pci_host . c : Avoid shifting left into sign bit
block : Remove redundant assertion
ivshmem : check shm isn ' t already initialized
tcg : correct 32 - bit tcg_gen_ld8s_i64 sign - extension
spapr / htab : fix savevm
vhost : fix dirty page handling
sockets : Plug memory leak in socket_address_flatten ()
tap : clear vhost_net backend on cleanup
linux - user : getpriority errno fix
io : fix check for handshake completion in TLS test
qemu - img : Plug memory leak in convert command
xen / pt : Set is_express to avoid out - of - bounds write
block : Catch ! bs -> drv in bdrv_check ()
exec : always use MADV_DONTFORK
dma : rc4030 : limit interval timer reload value
json - parser : Fix potential NULL pointer segfault
thread - pool : avoid deadlock in nested aio_poll () calls
qemu - option : Fix uninitialized value in append_option_parameter
linux - user : Correct type for BLKSSZGET
usb - redir : Fix crash on migration with no client connected
block / ssh : Avoid segfault if inet_connect doesn ' t set errno .
vnc : sanitize bits_per_pixel from the client
posix - aio - compat : Plug memory leak on paio_init () error path
ivshmem - server : fix possible OVERRUN
char : fix missing return in error path for chardev TLS init
target - s390x : Mark check_privileged () as ! CONFIG_USER_ONLY
qemu - config : fix leak in query - command - line - options
spapr_drc : Fix potential undefined behaviour
fix return check for KVM_GET_DIRTY_LOG ioctl
block / vvfat : Plug memory leak in check_directory_consistency ()
hw / arm : Mark the " fsl , imx6 " device with user_creatable = false
usb : xhci : no DMA on HC reset
block / iscsi : fix memory corruption on iscsi resize
target - i386 : CPUID : return highest basic leaf if eax > cpuid_xlevel
hw / scsi / spapr_vscsi : Remove superfluous memset
fix double free the memslot in kvm_set_phys_mem
vfio / pci : Fix regression in MSI routing configuration
input : fix memory leak
target - i386 : avoid overflow in the tsc - frequency property
blockjobs : fix set - speed kick
qemu - char : don ' t leak opts on error
target - arm : check that LSB <= MSB in BFI instruction
spapr : Move memory hotplug to RTAS_LOG_V6_HP_ID_DRC_COUNT type
qemu - ga : become_daemon (): reopen standard fds to / dev / null
target / ppc : Fix wrong number of UAMR register
cpu : Don ' t clear cpu -> exit_request on reset
ivshmem : Fix fd leak on error
vmdk : fix double free
hw / intc / arm_gic : Fix NVIC assertion failure
target - ppc : Fix use of uninitialized TCG variable in tlbiva
Initialize all bits in the sigaction .
extra check in mg_start (): options may be NULL
missing list close
Correcting a comment .
added Jonatan Alam � as catalan translator
fix possible OOB write in gif2tiff . c
* tools / tiffcrop . c : fix readContigStripsIntoBuffer () in - i ( ignore ) mode so
systemctl : fix memory leak (# 7289 )
sd - login . c : fixup for d70964d0
sysusers : fix memleak (# 4430 )
ude_rules . c : fix possible NULL pointer dereference in get_key ()
util : cunescape - fix memleak in error path
terminal / screen : perform bold -> light conversion only on foreground
basic / strv : use existing qsort_safe () helper
sd - bus : fix object tree to be deeper than 2 levels
fstab - util : detect out - of - range pri = assignments
bus : in sd_bus_try_close () consider local read queue too
resolved : use dns_answer_size () where appropriate to handle NULL DnsAnswer
Call all configured exec_dump_fn ' s when SIGUSR2 is received
app : clear current path from the context after copying it
app : update ` gimp - xcf - save ` procedure documentation .
Bug 701120 : Opening fuzzed . pnm - file causes stack buffer overflow
Remove a " register " keyword from C ++ code
fixed bug # 1661 Compiling -- pstack fails with binutils 2 . 13 . 90
do not ignore first char , when truncating spaces in str -> num conversion .
branches / 5 . 1 : Use btr_free_root () instead of fseg_free () for
bugfix : delayed insert table was using other table ' s expr_arena
[ trunk ] Fix simple warning about sign conversion
Fix a NULL pointer dereference in parse_server_transport_line ().
Fix a check - spaces complaint
initialize memory to remove Valgrind warning ( Zooko , Jeffrey Walton )
layouts : fix memory leak
Prevent slurmctld from dumping core ifjob_resrcs is missing in the
Initialize variable to eliminate clang error
Missing batch job given exit code of 1
Changed default styleObj offsets to 0 , duh ...
Fix the previous change .
[ coop handles ] Fix monotonic handle check race with stw in mono_handle_new
[ runtime ] Free leaked variable . (# 4643 )
[ sgen ] Fix names of descriptor types in describe_ptr ().
Replace a g_assert with a g_error .
Ignore seq points of entry and exit IL offsets .
[ runtime ] Make mono_trace_set_printerr_handler () call g_set_printerr_handler (). Fixes # 31451 .
* mini - s390 [ x ]. c ( is_regsize_var ): Support PTR / FNPTR too .
auth : Reset the TSIG state between queries
Prevents a possible memory leak
have to call delete_files before done ; oops
Added # prog_id import syntax for TSs without SDT
fix for v1 infe ( MPEG conformance file )
sm - common : fixed resource leak
card - westcos . c : fixed potential resource leak
[ Fix ] Simplexmlelement should throw an exception rather than fatal
Fix a bug in handleAssertionEffects
linux : Make modal dialog show more smoothly
ospf6d : don ' t run DR election early on " ipv6 ospf6 priority "
forgot to free irep -> reps
eliminate an LLVM warning about an unused value
compose : fix infinite loop in parser on some inputs
xkbcomp : Don ' t explode on invalid virtual modifiers
xkbcomp : fix pointer value for FreeStmt
Fixed new bug due to a typo in the PNG00 fix
Fixed incorrect call to WriteBlob reported in # 490 .
fixed a server crash
fixed input problem , partly :)
mmv : use xstrdup instead of strdup ( for allocation tracking ).
mk_core : thread : fix ucontext . h path
[ ShellApp ] Signal windows - changed when removing a window
reject pcre modifiers U with B
get_url_file_name : Fixed crash on OOM on debug build
hostip . c : return immediately from Curl_resolv_timeout () upon expired timeout .
Fixed GL error with SDL on startup if not using nvidia card .
m_stats : don ' t try to access bl_stats if it doesn ' t exist
[ git / 2 . 1 regression ] Fix opj_write_tile () failure when numresolutions = 1
fix error from commit 13585f15c7f7dc28bbbba1661efb280d530d114c
Unauthenticated DH client certificate fix .
fix previous modification -- if ssl -> cert is NULL , don ' t follow the pointer .
fix small memory leak with string - type templates
cast DWORD to int to match % d printf specifier
Try to fix # 302 .
Refresh the log window on pool failure message at startup .
Fixed one byte stack overflow in mcast recvfrom .
ccpp : open file for dump_fd_info with O_EXCL
refine addr_header comparison
src / pcm . c : Improve error reporting .
[ trunk ] added box length inconsistency check
make pigz work on . spl files , otherwise pigz can ' t breaks with kiwi
add missing g_free
set_interface_var () doesn ' t check interface name and blindly does
Closing connection after mg_send_digest_auth ()
CID 90884 storage_db : Check for bad data type
mobilesync : Plugging a memory leak
jpc_t1cod : check whether passno is in range
mips : return Fail on assert failure in MipsDisassembler . c
rgw / rgw_rest . cc : fix 4K memory leak
fixed non - root rooted path issue
mds : handle MMonMap
rgw : add xml output header in RGWCopyObj_ObjStore_S3 response msg
rgw / rgw_rest . cc : fix uninitialized static struct
rgw : AbortMultipart request returns NoSuchUpload if the meta obj doesn ' t
osd : fix rare memory leak
OutputDataSocket : fix uninit var
msg : MHeartbeat has a private destructor
msgr : fix problem with forced stop of pipe
osd : EINVAL when rmw_flags is 0
config option for mon ' s DNS SRV records
Exit the max size loop when there ' s nothing left possible to find
Issue 23433 in oss - fuzz : Heap - buffer - overflow in findNextBorderPixel ()
fix memory leak .
rsvg_characters_impl (): Unref the chars node we just created
apps / sipdial : add support for sips : URI scheme
Initialize pointer to NULL
Applying InnoDB snapshot
stole magnus ' fix for random im test failures from 5 . 1 - maint
typo in libedit fixed - possible buffer overflow - bug # 4696
fixed bug # 1661 Compiling -- pstack fails with binutils 2 . 13 . 90
Fixed a problem with Innodb_buffer_pool_pages_latched status variable .
cleaned up lots - o - warnings build
set correct cert type in CertificateRequest when using ChaCha suite with ECDSA
[ ARM ] export copy_page
sh : Use __GFP_ZERO for dma_generic_alloc_coherent ().
module : printk message when module signature fail taints kernel .
[ IPSEC ]: Fix inetpeer leak in ipv4 xfrm dst entries .
rmap : recompute pgoff for unmapping huge page
mtd : nand_bbt : set the smallest size of bbt table
cgroup : make sure memcg margin is 0 when over limit
KVM : PPC : e500 : Eliminate preempt_disable in local_sid_destroy_all
xen / pciback : Check PF instead of VF for PCI_COMMAND_MEMORY
loop : Update mtime when writing using aops
mtd : orion_nand : Fix build failure caused by typo
[ S390 ] cio : fix orb initialization in cio_start_key
iwlagn : check beacon frame size
Input : imx6ul_tsc - check for negative return value
brcmfmac : remove ( ab ) use of NL80211_NUM_ACS
brcm80211 : smac : set interface down on reset
staging : lustre : info leak in lnet_ping ()
pm2fb : reset transparency settings
bsg : fix bsg_poll () to return POLLOUT properly
[ IA64 ] Fix trap # 45 handling
[ SCSI ] ps3rom : sector size should be 512 bytes
Staging : cxt1e1 : use netdev_priv to fix build
pinctrl : sirf : Remove gpiochip on failure cases
ath10k : don ' t call quiet mode if it ' s not implemented
xen / p2m : Fix one - off error in checking the P2M tree directory .
ARM : SPEAr : conditionalize l2x0 support
[ TG3 ]: Update version to 3 . 82 .
vfs : do_last (): only return EISDIR for O_CREAT
KVM : SVM : Set the ' busy ' flag of the TR selector
vxge : Fix a possible memory leak in vxge_hw_device_initialize ().
ath9k : Send AUTHORIZED event only for station mode
Staging : crystalhd : use vfree () instead of kfree ()
ath5k : fix NULL pointer in antenna configuration
[ PATCH ] KVM : fix bogus pagefault on writable pages
perf inject : Do not repipe attributes to a perf . data file
fib6 : use FIB_LOOKUP_NOREF in fib6_rule_lookup ()
ceph : drop CAP_LINK_SHARED when sending " link " request to MDS
USB : xhci : No GFP_KERNEL in block error handling
[ media ] hdpvr : Fix memory leak
Staging : vt6655 : add missing free_netdev () on error in hostap_enable_hostapd ()
lib : correct 842 decompress for 32 bit
PCI : spear : Fix Section mismatch compilation warning for probe ()
md : check the return of mddev_find ()
drivers / serial / uartlite . c : Add missing of_node_put
Fix an OOPS when initializing IR remote on saa7134
ASoC : wm8731 : fix wm8731_check_osc () connected condition
drbd : fix if (); found by kbuild test robot
IPoIB : Free child interfaces properly
[ SCSI ] libfc : Fix e_d_tov ns -> ms scaling factor in PLOGI response .
spi : s3c64xx : Zero dma_slave_config struct in prepare_dma ()
stmmac : fix csr clock divisor for 300MHz
af_iucv : support ancillary data with HS transport
mwifiex : use maximum ssid length as 0xfe for p2p
vfs : fix spinning prevention in prune_icache_sb
kdb : break out of kdb_ll () when command is terminated
md : allow a partially recovered device to be hot - added to an array .
f2fs : don ' t need to sync node page at every time
tracing : Fix event format export
[ media ] em28xx : fix em28xx - input removal
netrom : Fix nr_getname () leak
ipv4 : warn once on passing AF_INET6 socket to ip_recv_error
sound : fix check for return value in snd_pcm_hw_refine
Btrfs : fix extent_buffer leak during tree log replay
gro : Fix bogus gso_size on the first fraglist entry
ACPICA : Fix to allow aliases within ASL namepaths
[ PATCH ] orinoco : Make nortel_pci_hw_init () static .
Staging : lustre : osc : Remove null check before kfree
ASoC : Intel : mfld - pcm : don ' t call trigger ops to DSP for internal streams
dmaengine : k3 : Remove chancnt affectations
x86 , mrst : Don ' t blindly access extended config space
mmc : sdhci - bcm - kona : Handle error from mmc_of_parse ()
rt2x00 : Shortcut link state updates when not operating as STA
sched : Try to deal with low capacity , fix update_sd_power_savings_stats ()
staging : brcm80211 : remove unnecessary cast in wlc_d11hdrs_mac80211
[ PATCH ] IB : fix CM use - after - free
md : skip all metadata update processing when using external metadata .
Btrfs : process mount options on mount - o remount ,
sh_eth : check TSU registers ioremap () error
ASoC : tlv320aic3x : Convert codec -> hw_read to snd_soc_read
[ PATCH ] selinux : Fix MLS compatibility off - by - one bug
ehca : convert cpu notifier to return encapsulate errno value
[ POWERPC ] EEH : Work with device endpoint , always
[ media ] fc2580 : silence uninitialized variable warning
Btrfs : should add a permission check for setfacl
rtlwifi : rtl8192cu : Fix error in pointer arithmetic
usb : dwc2 : gadget : ignore stall check for ep0
ipv4 : fix nexthop attlen check in fib_nh_match
gpio : potential null dereference
ASoC : Fix typo in 24 . 576MHz rate in WM5100
Staging : unisys : visorinput : Remove unused code
mtip32xx : Set queue bounce limit
rtlwifi : rtl8821ae : Fix problem with regulatory information
wl1271 : exit ELP mode when setting enabled rates in tx
intel_pmic_gpio : fix off - by - one value range checking
[ PATCH ] pcnet32 : remove incorrect pcnet32_free_ring
Staging : sep : fix time handler
perf hists : Don ' t consider filtered entries when calculating column widths
netrom : fix info leak via msg_name in nr_recvmsg ()
ipw2200 : support ETHTOOL_GPERMADDR
xen : correct error for building p2m list on 32 bits
dm - raid : silence compiler warning on rebuilds_per_group .
Input : wistron_btns - fix a memory leak in wb_module_init error path
dm era : check for a non - NULL metadata object before closing it
sctp : add a checking for sctp_sysctl_net_register
tools : iio : iio_event_monitor : check if event is fully read
drm / radeon / kms : add pll debugging output
[ IA64 ] Use set_cpus_allowed_ptr
[ SCSI ] bnx2fc : Drop incoming ABTS
include / linux / fs . h : fix userspace build
b43 : Fix swatch warning
Drivers : hv : hv_balloon : correctly handle val . freeram < num_pages case
ALSA : hda / jack - Fix the assignment of input jack - type
sky2 : print Optima chip name
igb : Avoid uninitialized advertised variable in eee_set_cur
[ S390 ] Fix timeval regression on s390
staging : comedi : disallow COMEDI_DEVCONFIG on non - board minors
staging : et131x : don ' t cast a void * to a struct pointer
spidernet : fix memory leak in spider_net_stop
perf hists browser : Fix segfault if use symbol filter in cmdline
[ XFS ] Fix hang after disallowed rename across directory quota domains
drm / radeon / kms : set fbdev_info for suspend / resume
usb : dwc3 : ep0 : move to CONFIGURED also on delayed status
toshiba_acpi : Fix mis - merge
i2c : designware : retry transfer on transient failure
mac80211 : fix memory leak
clk : ti : apll : not allocating enough data
tracing / filters : free pred when clearing filters
x86 - 64 : Don ' t use pointer to out - of - scope variable in dump_trace ()
leds : leds - cobalt - raq . c - use resource_size ()
batman - adv : remove broadcast packets scheduled for purged outgoing if
[ S390 ] missing check in dasd_eer_open .
drm / nouveau : remove pointless assignment
vhost - net : fix vq_memory_access_ok error checking
perf python : Initialize ' page_size ' variable
drm / i915 : make sure panel is sequenced off when starting a mode set
perf trace / scripting : Check return val of perl_run ()
USB : Deref URB after usbmon is done with it
ixgbe : Convert the normal transmit complete path to dev_consume_skb_any ()
HID : hyperv : match wait_for_completion_timeout return type
[ SCSI ] scsi_dh : Check for sdev state in store_dh_state ()
amd / powerplay : don ' t enable ucode fan control if vbios has no fan table
thinkpad_acpi : Fix a memory leak during module exit
USB : fix regression occurring during device removal
[ ALSA ] hda - codec - Fix NULL dereference in generic hda code
[ SCSI ] ibmvfc : Fix DMA mapping leak on memory allocation failure
[ SCSI ] mptfusion : Fix memory leak in mptctl_getiocinfo ()
perf : Fix the software events state check
perfcounters / powerpc : fix oops with multiple counters in a group
staging : wilc1000 : removes potential null dereference
iget : use iget_failed () in GFS2
[ media ] tda10071 : fix returned symbol rate calculation
ext4 : don ' t leave i_crtime . tv_sec uninitialized
ALSA : pcm : fix delta calculation at boundary wraparound
net / 9p : Initialize opts -> privport as it should be .
mt7601u : don ' t warn about devices without per - rate power table
staging : iio : Documentation : rewrite the right hand side of an assignment
xfs : catch invalid negative blknos in _xfs_buf_find ()
x86 : correct the CPUID pattern for MSR_IA32_MISC_ENABLE availability
OMAPDSS : HDMI : init output earlier
[ IPV6 ]: Fix crash in ip6_del_rt
ath9k / htc_drv_main : off by one error
[ TG3 ]: Fix PHY loopback on 5700
Btrfs : fix possible memory leak in btrfs_create_tree ()
ALSA : fix excessive background noise introduced by OSS emulation rate shrink
drm / i915 : check for supported depth at fb init time
zd1211rw : use unaligned safe memcmp () in - place of compare_ether_addr ()
perf tools : callchain : Fix spurious ' perf report ' warnings : ignore empty callchains
mlx4_core : Fix memory leak in mlx4_enable_msi_x ()
Input : amijoy - add missing platform check
ARM : imx : initialize clk_init_data . flags for clk - fixup - mux
NFSv4 : Return delegations synchronously in evict_inode
Input : ati_remote - add missing input_sync ()
[ media ] vb2 : Check if there are buffers before streamon
drm / msm : fix deadlock in bo create fail path
drm / mga200g : Use lockless gem BO free callback
[ ALSA ] Don ' t reject O_RDWR at opening PCM OSS with read / write - only device
[ SCSI ] Fix block queue and elevator memory leak in scsi_alloc_sdev
drm : shut up invalid edid messages
cpufreq : fix obvious condition statement error
acer - wmi : Add support for Aspire 1830 wlan hotkey
ipv4 : Fix fib_trie . c build , missing linux / vmalloc . h include .
libertas : down_interruptible () can return - EINTR , not EINTR
drm / i915 : try not to lose backlight CBLV precision
rt2x00 : rt2800lib : fix default TX power check for RT55xx
rsi : avoid format string leak to thread name
NFC : Clear pn533 target structure
drivers / net / wan : remove exceptional & on function name
usb : renesas_usbhs : fixup : avoid NULL access on error case pipe detach
greybus : core : fix hd - creation error path
cxlflash : Fix to prevent stale AFU RRQ
ath9k_hw : print device ID if not supported
bonding : in balance - rr mode , set curr_active_slave only if it is up
drbd : fix memory leak in drbd_adm_resize
tipc : Add in missing lock during link initialization
[ media ] saa7134 : v4l2 - compliance : clear reserved part of VBI structure
kernfs : update kernfs_rename_ns () to consider KERNFS_STATIC_NAME
mtd : mtk - nor : adjust sequence of trigger function and assignment function
cxl : Disable SPAP register when freeing SPA
hvc_xen : NULL dereference on allocation failure
mmc : mmci : fix mmci_post_request
[ media ] media : vb2 - memops : Export vb2_get_vma symbol
usb : dwc3 : gadget : stop touching HWO TRBs
pcmcia : fix incorrect bracketing on a test
pppoe : Lacks DST MAC address check
dmaengine : pl330 : Check if the DMA descriptor is NULL
x86 : Use string instruction memcpy on AMD Fam11h
perf / x86 : Check perf_callchain_store () error
isdn : mISDN : set -> family in -> getname ()
drivers / rtc / rtc - rv3029c2 . c : fix disabling AIE irq
cfg80211 : fix truncated IEs
rtlwifi : rtl8192se : Fix gcc 4 . 7 . x warning
radiotap : fix bitmap - end - finding buffer overrun
dm bufio : initialize read - only module parameters
ceph : properly set issue_seq for cap release
ACPICA : Fix memory leak in table load error path
UBIFS : check return code
spi : pxa2xx : missing break in pxa2xx_ssp_get_clk_div ()
ieee802154 : Fix oops during ieee802154_sock_ioctl
iwlwifi : fix TX tracer
ARM : mx51 : Fix the address space length for SSI
Drivers : hv : vmbus : Fix a bug in vmbus_open ()
usb : gadget : net2280 : Fix NULL pointer dereference
blk - mq : stop hardware queue in blk_mq_delay_queue ()
[ media ] cxd2820r : wait demod lock for DVB - C too
[ libata ] sata_sil : fix used - uninit warning
dma : pl330 : Set residue_granularity
firewire : ohci : correct signedness of a local variable
loop : fix NULL dereference if mount fails
drm / exynos : exynos_hdmi : Pass correct pointer to free_irq ()
staging : slicoss : use status in slic_ioctl
drm / i915 : Enable GMBUS for post - gen2 chipsets
skge : Adjust a null pointer check in skge_down ()
[ PATCH ] m68knommu : switch 68360 to using rtc_time
gpio : davinci : Fix possible NULL pointer deference
w1 - call request_module with w1 master mutex unlocked
axp288_fuel_gauge : Add original author details
slab : Use proper formatting specs for unsigned size_t
USB : sierra : fix urb and memory leak in resume error path
sh_eth : enable wake - on - lan for sh7734
gpio : mcp23s08 : request a shared interrupt
ALSA : hda - Sync EAPD with vmaster on AD1984A Thinkpads
cxl : remove redundant increment of hwirq
drm / radeon : don ' t setup audio on asics that don ' t support it
staging : lustre : dne : setdirstripe should fail if not supported
Staging : ozwpan : prevent bogus dereference
spi : fix spi_s3c24xx_gpio device handle lookup
Btrfs : return error code in btrfs_check_trunc_cache_free_space ()
igb : cap interrupts at 20K per queue when in itr mode 3
staging : ft1000 : fix kernel info leak to userland for statistics
ext3 : destroy sbi mutexes in put_super
ALSA : hda - check NULL pointer when creating SPDIF controls
usb : chipidea : udc : zero - length packet is only needed for TX
geneve : Unregister pernet subsys on module unload .
x86 , quirk : Fix SB600 revision check
net : avoid NULL deref in inet_ctl_sock_destroy ()
[ Bluetooth ] Revert session reference counting fix
[ media ] em28xx - i2c : rt_mutex_trylock () returns zero on failure
pktgen : fix crash with vlan and packet size less than 46
Staging : slicoss : changes memory allocation style in slicoss . c
mtd : docg3 : fix BCH registers
KVM : s390 : implement KVM_CAP_NR / MAX_VCPUS
virtio_console : Don ' t access uninitialized data .
f_phonet : fix page offset of first received fragment
[ SCSI ] qla2xxx : Correct IOCB queueing mechanism for ISP54XX HBAs .
[ SCSI ] nsp_cs : fix buf overflow
staging : unisys : remove S8 type
PCI : Use dev_is_pci () to identify PCI devices
ASoC : rt5645 : Check if codec is initialized in workqueue handler
phy : Add reset callback for not generic phy
f2fs : fix wrong max cost initialization
USB : missing error check in emi26
perf bench : Fix NULL pointer dereference in " perf bench all "
qxl : check for kmap failures
[ IPV4 ]: Make tcp_input_metrics () get minimum RTO via tcp_rto_min ()
selinux : fix off - by - one in setprocattr
greybus : uart : Fix the memory leak in connection init
Input : rohm_bu21023 - fix handling of retrying firmware update
[ ALSA ] ca0106 - Add missing sysfs device assignment
f2fs : initialize newly allocated dnode structure
slab numa fallback logic : Do not pass unfiltered flags to page allocator
drm / radeon / kms / DCE4 . 1 : ss is not supported on the internal pplls
input : pmic8xxx - pwrkey : Do not use mfd_get_data ()
e1000e : Correctly include VLAN_HLEN when changing interface MTU
mm , fs : check for fatal signals in do_generic_file_read ()
drm : WARN when removing unallocated node
mac80211 : fix rate control initialisation
iio : ti_am335x_adc : Fix : Use same step id at FIFOs both ends
cpuset : fix RCU lockdep splat in cpuset_print_task_mems_allowed ()
mfd : Fix return type of lm533 attribute is_visible
mfd : cros_ec : Instantiate ChromeOS EC character device
dm stats : fix a leaked s -> histogram_boundaries array
clk : mvebu : fix setting unwanted flags in CP110 gate clock
unix : use wq_has_sleeper in unix_dgram_recvmsg
[ TCP ]: Fix TCP header misalignment
mailbox , pl320 - ipc : remove __init from probe function
drm : Return EBUSY if the framebuffer is unbound when flipping .
clk : max77686 : fix number of clocks setup for clk_hw based registration
acpi - cpufreq : Do not load on K8
r8152 : check RTL8152_UNPLUG for rtl8152_close
mtd : docg3 : fix debug log verbosity
[ PATCH ] 2 . 6 . 17 missing a call to ieee80211softmac_capabilities from ieee80211softmac_assoc_req
Staging : sep : potential buffer overflow in ioctl
spi_mpc8xxx : Fix uninitialized variable
KVM : PPC : Initialize linears with zeros
ASoC : Do not write to invalid registers on the wm9712 .
pkt_sched : Add queue stopped test back to qdisc_run ().
ASoC : Intel : Skylake : Initialize NHLT table
mac80211 : fix register_hw error path
x86 / PCI : don ' t bother with root quirks if _CRS is used
ARM : 6891 / 1 : prevent heap corruption in OABI semtimedop
iommu / amd : Fix BUG when faulting a PROT_NONE VMA
iwlagn : save the latest smps mode
[ media ] tm6000 : Don ' t try to use a non - existing interface
fcoe : ensure that skb placed on the fip_recv_list are unshared
dmaengine / ste_dma40 : fix introduced warnings
ATM : mpc , fix use after free
xfrm : Don ' t allow esn with disabled anti replay detection
f2fs : fix inconsistency of block count during recovery
tg3 : Set initial carrier state to off .
cgroups : make unlock sequence in cgroup_get_sb consistent
drm / i915 : make sparse happy for the new vlv mmio read function
iio : ad5755 : fix off - by - one on devnr limit check
dma : edma : Fix memory leak
[ PATCH ] powerpc : make OF interrupt tree parsing more strict
usb : otg : Make USB3319 ULPI ID generic
mac80211 : avoid aggregation for VO traffic
fbdev : bf537 - lq035 : add missing blacklight properties type
chelsio : error path fix
Btrfs : fix memory leak in btrfs_parse_early_options ()
UBI : return on error in rename_volumes ()
drivers / net / wireless / b43 / main . c : 4351 : Fixed coding style
[ PATCH ] dm table split_args : handle no input
staging : unisys : visorchannel : Place logical continuation at the end of a line
tun : update file current position
vhost : copy_to_user -> __copy_to_user
sfc : check for allocation failure
r8152 : check tx agg list before spin lock
gro : Reset dev pointer on reuse
slub : Zero initial memory segment for kmem_cache and kmem_cache_node
quota : Fix WARN_ON in lookup_one_len
ARM : KVM : Bugfix : vgic_bytemap_get_reg per cpu regs
dm log writes : move IO accounting earlier to fix error path
mtd : elm : Use correct check on return value of pm_runtime_get_sync
staging : dwc2 : fix thinko in dwc2_hc_set_even_odd_frame ()
staging : iio : compress return logic
ext4 : verify extent header depth
Btrfs : fix bitmap size tracking
drm : mxsfb : Fix crash when provided invalid DT bindings
staging : ozwpan : Check error condition before creating endpoint .
do_wait () wakeup optimization : child_wait_callback : check __WNOTHREAD case
Staging : comedi : rtd520 : &&/|| typo
[ PATCH ] s3c2410fb : fix un - initialised dev field
dm thin : fix a memory leak when passing discard bio down
usb : xhci - plat : properly handle probe deferral for devm_clk_get ()
[ SPARC ]: Fix exec failures on sun4c .
USB : OHCI : don ' t allocate HCCA atomically
[ PATCH ] uml : activate_fd : return ENOMEM only when appropriate
dmaengine : rcar - dmac : use result of updated get_residue in tx_status
mm : batch - free pcp list if possible
tokenring / 3c359 . c : Prevent possible mem leak when open failed
ceph : cleanup aborted requests when re - sending requests .
[ media ] usbvision : fix locking error
airo : return from set_wep_key () when key length is zero
[ ALSA ] Fix possible deadlocks in sequencer at removal of ports
net : call newid / getid without rtnl mutex held
ASoC : SOF : ipc : Fix memory leak in sof_set_get_large_ctrl_data
ath5k : use tasklet_hi_schedule for beacon queue
KVM : MCE : Add MCG_SER_P into KVM_MCE_CAP_SUPPORTED
KVM : nSVM : Fix IOIO size reported on emulation
modedb : fix incorrect sync and vmode flags for CVT modes
[ PATCH ] net / pcnet32 : replace schedule_timeout () with msleep_interruptible ()
mac80211 : reduce stack usage in sta_ht_capa_read ()
[ media ] omap3isp : Handle omap3isp_csi2_reset () errors
[ ALSA ] ASoC : Fix TLV320AIC3X mono line output interconnect
usb : musb : host : fix for musb_start_urb Oops
mwifiex : Fix possible buffer overflows in mwifiex_cmd_802_11_ad_hoc_start
defxx . c : dfx_bus_init () is __devexit not __devinit
staging / xgifb : Remove unnecessary binary calculation in GetXG27FPBits
mwifiex : corrections in PCIe event skb handling
mac80211 : fix uninitialized variable
proc : fix oops on invalid / proc /< pid >/ maps access
[ PATCH ] struct path : convert ieee1394
btrfs : fix in - memory value of total_devices after seed device deletion
md : allow a partially recovered device to be hot - added to an array .
USB : serial : fix error message on close in generic driver
[ SCSI ] Fix uninitialised width and speed in sym2
rcu : add DEBUG_OBJECTS_RCU_HEAD check for alignment
Btrfs : fix memory leak in start_transaction ()
[ IA64 ] Prevent sn2 ptc code from executing on all ia64 subarches
[ PATCH ] cciss : set default raid level when reading geometry fails
dm : fix the second dec_pending () argument in __split_and_process_bio ()
[ media ] ascot2e : Fix I2C message size check
ext4 : fix incorrect block reservation on quota transfer .
[ ALSA ] Fix errors with user TLV_WRITE
fs : allow d_instantiate to be called with negative parent dentry
[ PATCH ] orinoco : Fix memory leak on error in processing hostscan frames .
tipc : check nl sock before parsing nested attributes
ipv4 : rate limit updating of next hop exceptions with same pmtu
kvm : x86 : fix emulator buffer overflow ( CVE - 2014 - 0049 )
mtd : spear_smi : clear status register on init
wl12xx : fix a memory leak of probereq template upon recovery
signals , ptrace , sched : Fix a misaligned load inside ptrace_attach ()
Staging : rtl8192e : Fix potential NULL pointer dereference
ip6erspan : make sure enough headroom at xmit .
staging : rtl8188eu : use eth_broadcast_addr () in place of memset
f2fs : avoid wrong error during recovery
floppy : silence warning during disk test
ext4 : prevent stack overrun in ext4_file_open
iommu : Stop deferring probe at end of initcalls
mm : compaction : do not compact pgdat for order - 0
futexes : Fix infinite loop in get_futex_key () on huge page
staging : slicoss : Fix possible reuse of freed memory in timer function
staging : lustre : race condition for check / use cfs_fail_val
amd - xgbe : fix potential memory leak in xgbe - debugfs
uwb : uwbd () is not freezable kthread
[ PATCH ] irq - devres : fix failure path of devm_request_irq ()
staging : ks7010 : use u16 instead of unsigned short in hostif_data_indication
slub : Use NUMA_NO_NODE in get_partial
drm / nouveau : require contiguous bo for framebuffer
getname_kernel () needs to make sure that -> name != -> iname in long case
libceph : must hold mutex for reset_changed_osds ()
iio : adc : mcp3422 : Checking for error on probe
ARM : pxa : Improve a size determination in pxa3xx_u2d_probe ()
[ PATCH ] USB : resolve Zaurus problem
sdio : handle null tuples
[ SCSI ] mpt2sas : Bump version 08 . 100 . 00 . 00
UBIFS : avoid kernel error if ubifs superblock read fails
staging : comedi : ni_labpc : fix a > 80 char line issue
ssb : cc : use correct min_msk for 0x4312
ASoC : topology : Fix memory leak in widget creation
staging , rts5208 , LLVMLinux : Change extern inline to static inline
drm / i915 : Start WM computation from scratch on ILK - BDW
drm / i915 / cnl : Reprogram DMC firmware after S3 / S4 resume
Input : gpio_keys - fix a memory leak
gpu : host1x : clk_round_rate () can return a zero upon error
mqueue : fix a use - after - free in sys_mq_notify ()
skd : Use kmem_cache_free
perf / x86 / intel / uncore : Correct fixed counter index check for NHM
leds - atmel - pwm . c : Make pwmled_probe () __devinit
[ PATCH ] nfsd4 : renew lease on seqid modifying operations
ARM64 : PCI : Fix struct acpi_pci_root_ops allocation failure path
KVM : x86 : fix error return code in kvm_arch_vcpu_init ()
powerpc / ptrace : Fix brk . len used uninitialised
ALSA : correct kcalloc usage
af_key : Do not call xfrm_probe_algs in parallel
Btrfs : truncate pages from clone ioctl target range
vfio / spapr_tce : Check kzalloc () return when preregistering memory
gpio - pch : cleanup __iomem annotation usage
drm / amd / display : Do DC mode - change check when adding CRTCs
iwlwifi : mvm : request statistics when reading debugfs
[ ALSA ] hda - codec - Fix unbalanced mutex
ath9k : Fix TX power calculation
ASoC : AMD : Add NULL pointer check
[ X25 ]: Restore skb -> dev setting in x25_type_trans ().
ovl : fix dentry reference leak
sunvnet : fix NULL pointer dereference
exit : proc : don ' t try to flush / proc / tgid / task / tgid
mac80211 : fix AP - mode frame matching
sm501fb : deallocate colormap only if allocated
drm / amd / display : fix uninitialized variable warning
[ media ] Don ' t OOPS if videobuf_dvb_get_frontend return NULL
igb : fix vf lookup
ARM : EXYNOS : fix GIC using for EXYNOS5440
dccp : fix freeing skb too early for IPV6_RECVPKTINFO
dmaengine : device must have at least one channel
ACPI : silence kmemcheck false positive
ARM : davinci : da830 - evm : specify the chipselect in davinci_nand_pdata
Btrfs : don ' t run __tree_mod_log_free_eb on leaves
mailbox : add MODULE_LICENSE () for mtk - cmdq - mailbox . c
HID : sensor - hub : fix potential memory leak
orinoco : fix TKIP countermeasure behaviour
exofs : use bio_clone_fast in _write_mirror
xfs : fix missing ILOCK unlock when xfs_setattr_nonsize fails due to EDQUOT
GFS2 : Fix freeze issue
[ PATCH ] x86 - 64 : Fix C3 timer test
ALSA : usb - audio - clear chip -> probing on error exit
[ PARISC ] set memory ranges in N_NORMAL_MEMORY when onlined
i2c : mux : reg : wrong condition checked for of_address_to_resource return value
xfs : validate acl count
Btrfs : fix race between readahead and device replace / removal
staging / xgifb : Fix return of uninitialized variable
clk : versatile : fix memory leak
ath9k_htc : Fix free slot value for cab queue
tick / nohz : Fix possible missing clock reprog after tick soft restart
[ PATCH ] autofs4 : need to invalidate children on tree mount expire
staging : comedi : das16 : change MODULE_DESCRIPTION
mmc : mmci : Cascade EPROBE_DEFER from regulators .
HID : hidraw : Fix crash on HIDIOCGFEATURE with a destroyed device
Driver Core : don ' t oops with unregistered driver in driver_find_device ()
tcm_loop : Fix memory leak in tcm_loop_submission_work error path
DLM : fix overflow dlm_cb_seq
tty : Fix GPF in flush_to_ldisc ()
[ XFS ] Fix nused counter . It ' s currently getting set to - 1 rather than
[ SCSI ] ibmvscsi : Add 16 byte CDB support
[ SCSI ] qla4xxx : a small loop fix
ALSA : hda / realtek - Fix possible Oops with NULL input_mux
pch_dma : fix error return code in pch_dma_probe ()
coresight : fix kernel panic caused by invalid CPU
usb : gadget : f_ecm : fix missing unlock on error in ecm_alloc ()
md : make recovery started by do_md_run () visible via sync_action
bpf : fix missing error return in check_stack_boundary ()
typhoon : Add missing firmware copy .
crypto : dcp - Check the return value from devm_ioremap_resource ()
mei : start disconnect request timer consistently
drm : missing drm_vm . c changes for consistent maps
generic - adc - battery : Fix checking return value of request_any_context_irq
i2c : ismt : prevent memory corruption in ismt_access ()
tcp : skb_mstamp_after helper
mtd : spi - nor : Off by one in cqspi_setup_flash ()
drivers / net / fs_enet : remove null pointer dereference
ASoC : sgtl5000 : defer the probe if clock is not found
btrfs : add check to sysfs handler of label
video : uvesafb : Fix integer overflow in allocation
timekeeping : Make local variables static
mtd : onenand : omap2 : Remove skip initial unlocking support
UBI : always re - read in case of read failures
drm / i915 : Discard objects from mm global_list after being shrunk
mac80211 : fix ieee80211_sta_process_chanswitch for 5 / 10 MHz channels
iio : imu : adis16400 : release allocated memory on failure
[ PATCH ] e1000 : Enable polling before enabling interrupts
dmaengine : imx - sdma : fix up param for the last BD in sdma_prep_slave_sg ()
iio : buffer : Free active scan mask in iio_disable_all_buffers ()
scsi : hpsa : do not reset enclosures
ext4 : fix deadlock with quota feature
hwmon : ( asb100 ) Fix vrm write operation
vesafb : fix memory leak
ext4 : check for zero length extent
be2net : fix pmac_id for BE3 VFs
phonet : refcount leak in pep_sock_accep
mm : Fix NULL pointer dereference in madvise ( MADV_WILLNEED ) support
s390 / cpumf : add missing lpp magic initialization
pinctrl : pinconf : fix comparison of different types
drm / nouveau / devinit / nv04 : change owner to int
iwlwifi : mvm : disable scheduled scan
drm / nouveau / bios / gp104 : initial support
ASoC : Fix incorrect kfree in wm8731_probe error path
ext4 : fix potential integer overflow
fs : clear writeback errors in inode_init_always
KVM : PIT : control word is write - only
TTY : hvcs : fix missing unlock on error in hvcs_initialize ()
mfd : Add free max8998 -> ono irq in max8998_irq_exit ()
KVM : PPC : Book3S : Check KVM_CREATE_SPAPR_TCE_64 parameters
stmmac : add missing of_node_put
ARM : omap - device : add missed callback for suspend - to - disk
ieee1394 : SPIN_LOCK_UNLOCKED cleanup
mac80211 : fix a possible leak of station stats
ceph : fix cap_wanted_delay_ { min , max } mount option initialization
tcp : fix TSO FACK loss marking in tcp_mark_head_lost
fs / cifs / cifs_dfs_ref . c : fix potential memory leakage
ipmi : Fix memory leak in __ipmi_bmc_register
[ PATCH ] kyrofb : Fix uninitialized value
aha1542 : Convert aha1542_intr_reset to function
mfd : Mark two lm3533 zone registers as volatile
fcoe : make sure fcoe frames are unshared prior to manipulating them
sunrpc : don ' t register UDP port with rpcbind when version needs congestion control
max17042_battery : Handle irq request failure case
batman - adv : Add BATADV_DBG_TP_METER to BATADV_DBG_ALL
[ media ] cx231xx : prints error code if can ' t switch TV mode
sctp : Use correct sideffect command in duplicate cookie handling
jfs : prevent NULL deref in diFree
memcg : fix leak of accounting at failure path of hugepage collapsing
xfrm_user : fix info leak in copy_to_user_state ()
Staging : comedi : off by one in doDevConfig ()
staging : mt7621 - dma : Fix ident by space
cgroup : add sparse annotation to cgroup_iter_start () and cgroup_iter_end ()
ARM : mvebu : mvebu - soc - id : add missing clk_put () call
uprobes : split THPs before trying to replace them
drm / i915 : don ' t pwrite tiled objects through the gtt
[ SCSI ] libfc : fix payload size passed to fc_frame_alloc () in fc_lport_els_request
irda : fix overly long udelay ()
cpufreq_stats : do not remove sysfs files if frequency table is not present
OMAPDSS : fix registering the vsync isr in apply
bcache : fix use - after - free in btree_gc_coalesce ()
pNFS : Ensure we check layout validity before marking it for return
hv_netvsc : use kcalloc
ASoC : WM8903 : Free IRQ on device removal
firewire : cdev : fix another memory leak in an error path
drm : make drm_minors_cleanup () static
au1xxx - ide : use ide_find_port ()
tracing : Uninitialized variable in create_tracing_map_fields ()
ASoC : Intel : Skylake : Fix memory leak during init instance
ipv4 : fib : Export free_fib_info ()
drm / i915 : fix EDID memory leak in SDVO
Staging : rtl8187se : Remove unnecessary braces around single statement in dot11d . c
Input : powermate - fix oops with malicious USB descriptors
ptr_ring : fail early if queue occupies more than KMALLOC_MAX_SIZE
Input : return correct size when reading modalias attribute
RDS : Heap OOB write in rds_message_alloc_sgs ()
staging : lustre : llog : prevent out - of - bound index
KVM : VMX : fix page leak in hardware_setup ()
drm / amdgpu : it should disable gfxoff when system is going to suspend
staging : comedi : ni_6527 : reset edge detection registers
ipv6 : fix possible crashes in ip6_cork_release ()
[ PATCH ] s390 : bug in setup_rt_frame
cassini : fix off by one
KVM : avoid NULL pointer dereference in kvm_dirty_ring_push
ASoC : img - parallel - out : Add pm_runtime_get / put to set_fmt callback
sctp : Correctly track if AUTH has been bundled .
powerpc : Add missing prototype for hdec_interrupt
staging : iio : compress return logic
block : initialise bd_super in bdget ()
ALSA : hdspm - Fix PCI ID for PCIe RME MADI cards
[ SCSI ] ibmvfc : Driver version 1 . 0 . 4
ipv6 : Add GSO support on forwarding path
fm10k : rename mbx_tx_oversized statistic to mbx_tx_dropped
usb : gadget : fsl_qe_udc : Remove unneeded condition
staging / lustre / ofd : refill env in ofd_get_info
omap3 : beagle : Fix USB host port power control
ALSA : timer : Fix missing queue indices reset at SNDRV_TIMER_IOCTL_SELECT
cred : copy_process () should clear child -> replacement_session_keyring
Reset the output value on error in _gnutls_x509_dn_to_string ()
shaper : fix reallocation
parse : Check for non - empty command / rspfile / rspfile_content
Bug : Lua can generate wrong code when _ENV is < const >
Handle NO response without message properly
checkpath : fix allocation size of path buffer
[ sipmsgops ] fix codec_delete_XX () parsing ( 2 )
Fixed error concealment when no MBs are decoded in the current pic
demux : mp4 : fix buffer overflow in parsing of string boxes .
Prevent an OOB read in ` expand_dims . cc `
[ tflite ] Ensure ` MatchingDim ` does not allow buffer overflow .
fix possible format string exploit if packet dumping enabled
[ pdf ] Defer page unloading in JS callback .
Use the lock when accessing the buffer object .
Upgrade a TextIterator ASSERT to a RELEASE_ASSERT as a defensive measure .
Always call UpdateCompositedScrollOffset , not just for the root layer