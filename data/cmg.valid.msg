siddec : reset bytes counter
mpeg2dec : Fix buffer memory leak in error code path
daemon : ldm : avoid manual free ()
Turbostat : Use the ' count ' gauge to count SMIs
write_http plugin : Add assertion for scan - build .
rrdtool plugin : Wait for the ` queue thread ' to exit ..
Always stat profile files after reopening them
fall back on realm name as a default domain name
crypto : fix memory leak in randomBytes () error path
return with CL_EFORMAT instead of CL_EDSIG
Low : tools : crm_simulate - fix small xpath memory leak in inject_node_state ()
crmd : Provide useful feedback when the metadata is unreadable
High : PE : Parse fail - count correctly ( it may be set to ININFITY )
wallet : Add error handling . Check return value of ParseUInt32 (...) in ParseHDKeypath (...).
tree : Check for `. git ` with case insensitivy
Fix a double free on error recovering
daemon : unbreak NO_IPV6 build regression
pack - objects : fix early eviction for max depth delta objects
grep : fix colouring of matches with zero length
send - pack : avoid deadlock on git :// push with failed pack - objects
git - log : do not output excess blank line between commits
rev - list -- header : output format fix
setup . c : verify_non_filename (): don ' t die unnecessarily while disambiguating
builtin - commit : fix summary output .
lib - master : Minor memory leak fix on deinit .
doveadm proxy : Don ' t crash if remote doesn ' t support log proxying
lib - storage : Fix accessing the same " raw " mailbox multiple times in process .
data stack : Fixes to handling out - of - memory situations .
rfc822_parse_phrase (): Don ' t read outside data boundaries if input is empty .
imap : APPEND crashed if invalid keyword was given as parameter .
sdbox : In - memory mailbox GUID differed from on - index GUID after mailbox_create ().
imapc : Added assert to make sure callback is never NULL .
lib - program - client : Fix memory access problem
lib - storage : Fixed hooks with plugins that didn ' t override any vfuncs
lib - auth : Fix memory leak in auth_client_request_abort ()
lib - index : mail - index - transaction - export : Fixed handling of transaction_flag_updates_have_non_internal () return value .
memset () the returned structure , just in case .
authd : always use an empty buffer to read ident reply
openssl : fix boringssl build again
smb : Don ' t try to connect with empty credentials
read_tcp_data () fix to get the proper buffer pointer and size
sasl_sspi : Removed note about the NTLM functions being a wrapper
sftp upload : expire to advance state machine
free the allocated mailbox name at disconnect
avoid proxy ( and other ) auth if no user + password is given
Added header to momctl unit test to resolve do_mom
fixed unit test
introduced a new # include in response to Andrei ' s uri changes
app_python3 : skip execution of child_init () for rank PROC_INIT
acc ( k ): Replace call to ( deprecated ) atoi () function with
ao / wasapi : more consistent / reliable method of computing extra WAVEFORMATEXTENSIBLE size
vo_opengl : do not leak previous FBO when reallocating it
demux_lavf : mark ASS tracks as always UTF - 8
vo_opengl : error out gracefully when trying to use FBOs without FBO API
ad_spdif : Fix crash when spdif muxer is not available
don ' t call video -> get_info if it ' s NULL
vo_direct3d : allow resizing before video init
vo_rpi : fix blackscreen before the first subtitle / OSD is rendered
demux_libass : actually set priv context
reset channels too
Tables : Fix spelling mistake in comment
iASL : Update for ToPLD macro .
gtk : set free_scale when setting zoom_fit
block / cloop : Use g_free instead of free
lm4549 : Fix buffer overflow
fix full frame updates for VNC clients
virtio - 9p : Check the return value of llistxattr .
target - mips : log instructions start in TCG code
x86 : fix miss merge
bb # 11135 - sigtool now verifies cvd prior to unpacking
handle CL_ETIMEOUT in cl_strerror
bb # 11370 - do not update mirrors on custom database urls
add missing ' break '
libfdisk : gpt : avoid unnecessary pmbr check
blkid : add DEVNAME = to export output format
fdisk : non - linux support ( MAXPATHLEN )
accept NULL as a valid prefix in rpmdbOpen ()
don ' t assume write ( fd , buf , 0 ) always returns w / o error
rpmdbRemove () should free header it read
mailbox : add mkdir back to copyfile , and return IOERROR
http_dav : allow read - free - busy to be set via DAV
Modules : fix client blocking calls access to invalid struct field .
rtmpproto : Validate the embedded flv packet size before copying
avplay : clear pkt_temp when pkt is freed .
lavc : set avctx -> hwaccel before init
avconv : fix off by one check in complex_filter
tls : fix compilation when both gnutls and openssl are enabled
vf_gradfun : fix uninitialized variable use
handle http error codes
Fix a read past end of buffer crash in the mp3 probe
oma : check avio_read () return value
mxfdec : return error if no segments are available in mxf_get_sorted_table_segments
issue 10 - zmq_strerror problem on Windows
Quench reject_delay to be no less than zero
Fixed md -> grouplist memory leak .
Improved failure handling in jabber_si_handle_request (). If the imcb
From David Carne :
Gryphon : do not use col_set_str with non constant strings
tvbuff : fix compilation with gcc - 4 .
Initialize all tvb values in FieldInfo_get_range .
Qt : Fix leaked interfaces array in Wireless frame
Put volatile back - without it , the compiler warns you .
Fixed incompatible pointer type
Set the correct function state during execution . This is mainly to have
fix yet another possible segfault
fix : prevent possible crash
fix memory leak
Make it possible to disable post_max_size
isync with 5 branch
flatpak remote - ls - u : only consider apps from the current remote
Fixed a memory leak with alternative names
fuzz - unit - file : allow a space between variable name and "=" (# 8670 )
sd - daemon : verify NOTIFY_SOCKET path length
sysusers : fix memleak (# 4430 )
systemctl : fix detection of active units
systemctl : also highlight a load state of " not - found " as red
vconsole : copy font to 63 consoles instead of 15
vconsole : add copyright line
util : ignore_file should not allow files ending with '~'
journalctl : free arg_file on exit
acpi : make sure we never free an uninitialized pointer
swap : don ' t add too many deps for swap files form / proc / swap
path - util : when parsing a timestamp we don ' t know the timezone
journalctl : don ' t choke on entries with no MESSAGE = field
Check if header is there
k_gethostname -> gethostname
Fixed string size calculation in winpr_HexDump
Fixed memory leak .
Fixed argument check in < tls_disconnect >
Fixed windows command line
libwinpr : check return value of realloc .
winpr / file : add missing NULL check
Fixed leak of client random .
gdImageScaleTwoPass memory leak fix
Remove CINT remnant
fix warning on MacOS X .
wavpack : Fix an integer overflow
electronicarts : prevent overflow during block alignment calculation
avcodec / mimic : Fix runtime error : left shift of negative value - 1
lavu / opt : dont crash on freeing NULL AVOptionRanges
hevc_cabac : decrease CABAC_MAX_BIN
tests / audiomatch : Fix compile warning
mxfdec : fix memleak on mxf_read_close ()
snowenc : dont crash with gray but exit with an error msg .
Check the allocated pointer instead of the given pointer as intended
rv10 : Forward error from rv10_decode_packet
avcodec / sanm : Reset sizes in destroy_buffers ()
oggdec : Fix memory leak in setting up vorbis headers
dca : Read params -> pancABIT0 [ param_index ] only if part0 is set
avcodec / h264_mvpred : Fix multiple runtime error : left shift of negative value
libavcodec / exr : fix channel size calculation for uint32 channel
adpcmenc : check for coded_frame allocation failure
avcodec / ffv1dec_template : Fix signed integer overflow
avformat / utils : check for overflow before reallocating side data
mpegvideo : clear overread in clear_context
riff : fix invalid av_freep () calls on EOF in ff_read_riff_info
hls : fix integer overflow
avformat / avidec : Remove ancient assert
avoid uninitialised variable warning and clarify code
doc / examples / decode_video : Fix format string vulnerability
avcodec / diracdec : Fixes integer overflow
avcodec / wmv2enc : allocate padding for extradata and check malloc failure
avcodec / utils : check the private context class
mpegvideo_enc : reduce QMAT_SHIFT to avoid overflow in dnxhd
lavc / avpacket : check for malloc failure
fix infinite loop ( suggested change by rjayne at convera dot com )
avformat / rtspdec : Use av_mallocz_array ()
avcodec / mpegaudiodec_template : use av_freep (), do not leave stale pointers in memory
qdm2 : avoid integer overflow
avformat / http : return EOF from ff_http_do_new_request if previous response said Connection : close
drawtext : fix leak with timecodes
avcodec / truemotion2 : Fix integer overflows in tm2_high_chroma ()
avcodec / get_bits : add av_assert2 to get_bits_long ()
avcodec / mpeg12dec : check block index in mpeg2_fast_decode_block_non_intra ()
pthread_frame : fix uninitialized variable read
avcodec / get_bits : Fix get_sbits_long ( 0 )
avcodec / pthread_slice : Use av_freep () to avoid leaving stale pointers in memory
ituh263 : reject b - frame with pp_time = 0
mpevideo_enc : disallow multiple slices for h261 and flv
avcodec / jpeg2000 : Check that codsty -> log2_prec_widths / heights has been initialized
aasc : check before reading the first 4 byte , fix overread
Check for out of bound writes in the wmavoice decoder .
rv34 : fix signed multiplication overflow
tests / api / api - seek - test : Silence compiler warnings about uninitialized variables
lavfi / unsharp : fix opencl crash on 64bit linux
avcodec / dca_lbr : Fix off by 1 error in freq check
mpc8 : Check the seek table size parsed from the bitstream
avcodec / mlpdec : Fail if the input is too small
avformat / nistspheredec : initialize header_size to - 1
vf_overlay : silence warning : X may be used uninitialized in this function
avcodec / mpeg4videodec : Fix GMC with videos of dimension 1
pca : use sizeof ( variable ) instead of sizeos ( TYPE )
avcodec / vp56 : Check avctx -> error_concealment before enabling EC
avcodec / jpeglsdec : check shift for values that cause overflow later
golomb : Fix overread in get_ur_golomb_jpegls ()
avformat / pva : Check for EOF before retrying in read_part_of_packet ()
j2kdec : Fix integer overflow leading to a segfault
avformat / vobsub : fix NULL dereference
avutil / libm : fix isnan compatibility hack
avcodec / mips / aaccoder_mips : Disable ff_aac_coder_init_mips () to prevent build failure
fixed one mpeg decoder memory leak
mmsh : fix integer overflow in mmsh_seek ()
svq3 : check the watermark height .
avcodec / hevc_ps : Only discard overread VPS if a previous is available
avconv : Replace goto redo on decode fail with continue .
avformat / gxfenc : Clear nb_fields on array deallocation
vc1 : check image height , fix division by 0
vp3 : Check fps validity more completely
Remove code that duplicates existing code a few lines down .
avformat / oggparsespeex : Check for extradata allocation failure
init picture structure to avoid uninitialized fields
avcodec / mlpdec : Fix runtime error : left shift of negative value - 1
ffmpeg : use isatty () before messing with the terminal state
avformat / mpc8 : clear buffer padding area
Avoid a deadlock when decoding wma .
rtmpproto : Send chunk size on the network channel
av_dict_set : fix potential memory leak with AV_DICT_DONT_OVERWRITE
avienc : fix overflow of audio sample count
avcodec / mlpdsp : Fix undefined shift ff_mlp_pack_output ()
ffplay : avoid frame data leak on early frame drop
avcodec / scpr : Check for min > max in decompress_p ()
mov : Fix near infinite loop in stsd parsing .
avcodec / smc : Check remaining input
img2dec : dont set start_time / duration to invalid values
lzo : fix overflow checking in copy_backptr ()
avformat / mxfdec : fix typo in error check of mxf_edit_unit_absolute_offset ()
avformat / mov : use 64bit for size in mov_skip_multiple_stsd ()
http : Check for negative chunk sizes
af_channelmap : free old extended_data on reallocation
aacsbr : always initialize max_qmf_subbands
avcodec / takdec : Fix runtime error : left shift of negative value - 63
avcodec / mjpegdec : Check for rgb before flipping
mpeg12 : check for available bits to avoid an infinite loop
avcodec / vorbis_parser : Move vp check
vaapi_h264 : Set max_num_ref_frames to 1 when not using B frames
avcodec / escape124 : Check depth against num_superblocks
avcodec / movtextdec : Fix potential integer overflow
avformat / aacdec : Fix leak in adts_aac_read_packet ()
avcodec / ituh263dec : Fix runtime error : left shift of negative value - 22
wtv : Mark attachment with a negative stream id
avformat / cache : Use int64_t to avoid int overflow in cache_read
indeo4 : prevent printing uninitialized variable
tm2dec : fix overread
avformat / utils : free AVStream . codec properly in free_stream ()
lavf : Avoid using av_malloc ( 0 ) in av_dump_format
avcodec / ac3_parser : Check init_get_bits8 () for failure
lavf / mov : Do not overread iTunes metadata .
prevent integer overflow in calculating duration
libopenjpegdec : always check image because decoding may still fail
avcodec / mjpegdec : Check for pixfmtid 0x42111100 || 0x24111100 with more than 8 bits
aacsbr : Silence warning : max_qmf_subbands may be used uninitialized in this function
westwood_vqa : fix null pointer dereference
zmbv : Reset the decoder on keyframe errors
truemotion2 : error out if the huffman tree has no nodes .
vqf : check samplerate , avoid division by 0 .
aacdec : fix signed overflows in lcg_random ()
avcodec / xan : Improve overlapping check
avcodec_decode_audio : do not trust the channel layout , use the channel count .
cdxl demux : do not create packets with uninitialized data at EOF .
avformat / mpc8 : fix hang with fuzzed file
avplay : Do not print a possibly uninitialized value
mov : fix leaking memory with multiple drefs .
rtmpproto : make condition work in overflow case .
avcodec / ffv1dec : Free tables on init failure
avformat / avio : Fix null pointer dereference in case of memleak
avcodec / mjpegdec : Clip DC also on the negative side .
adxdec : Fix division by zero
h264 : check for integer overflow , fix null pointer dereference
asfdec_o : prevent overflow causing seekback
lavf : fix the comparison in an overflow check
mem : Handle av_reallocp (..., 0 ) properly
on2avc : Fix out of array access
crash fix by Carl Eugen Hoyos cehoyos ag . or at
rtp : Fix integer underflow that could allow remote code execution .
roqaudioenc : Fix crash with very small roq files
libavcodec / qsvenc . c : A warning message when library will work at partial hardware acceleration .
check region depth for validity
avcodec / bsf : fix resource leak in av_bsf_list_parse_str
avcodec / pngenc : Initialize fctl_chunk to 0
aiffdec : fix division by zero
aacsbr : fix integer overflow
fix sigfpe with intra only encoding
avcodec / shorten : Check k in get_uint ()
rtsp : Fix compiler warning for uninitialized variable
remove warnings about uninitialized vars
vorbis : Fix last quarter of CVE - 2011 - 3893
avformat / bethsoftvid : check 4th header byte in probe
ffmpeg : dont call exit_program () from a signal hander
avpriv_mpeg4audio_get_config : check init_get_bits () return code .
vorbisdec : do not leak the first frame .
avcodec / mpeg12dec : check for overread in mpeg1_fast_decode_block_inter ()
avcodec / msmpeg4dec : Check for input end in msmpeg4v34_decode_mb ()
sbgdec : dont set slide to a uninitialized value
svq1dec : dont export the qscale table .
swresample / swresample : Use av_mallocz_array ()
avutil / hwcontext_dxva2 : return an error when buffer allocation fails
avcodec / snowdec : Fix runtime error : left shift of negative value - 1
mpegvideo : initialize videodsp with correct pixel depth
mxg : fix compiler warning for uninitialized variables
avformat / nutdec : Check X in 2nd branch of index reading
Values > 4 for lowres corrupt output .
indeo3 : error out if no motion vector is set .
avplay : reset rdft when closing stream .
Fixed a possible bug , checked against the assembly .
avcodec / jpeg2000dec : Zero code and quant style
hlsproto : Properly close avio buffer in case of error
prores : Evaluate all the quantizers
swscale : fix signed overflow in yuv2mono_X_c_template
avcodec / pgssubdec : Use av_fast_padded_malloc () for rle buffer
loco : silence warning : decoded may be used uninitialized in this function
rv20 : prevent calling ff_h263_decode_mba () with unset height / width
Fixed invalid access in wavpack decoder on corrupted extra bits sub - blocks .
protect realloc overflow
lavf / mpeg : Initialize a stack variable used by memcmp ().
srtp : Mark a few variables as uninitialized
rtmpproto : Lengthen the filename buffer when receiving streams
avformat / oggdec : check for av_packet_new_side_data () failure
qtpalette : make the color_ * variables unsigned again
avcodec / mpegaudiodec_template : Correct return code on id3 tag discarding
asink_nullsink : plug a memory leak .
lavf / hlsenc : fix basename size computation off - by - one bug
indeo : Reject impossible FRAMETYPE_NULL
hnm4 : change width / height to int to fix hypothetical integer overflows
rtmp : Clean up properly if the handshake failed
avcodec / mjpegdec : Fix DC overflow in decode_block ()
lavf probe : prevent codec probe with no data at all seen
avcodec / flacenc : Fix Invalid Rice order
doc / examples / filtering_audio : init packet0 . data
avformat / rmdec : check against mismatching int4 interleaver parameters which would leave uninitialized holes
avcodec / ac3dec_fixed : Fix integer overflow in scale_coefs ()
libx264 : Define X264_API_IMPORTS on MSVC / ICL
shorten : allocate space for padding
libavformat / hdsenc : check init_file () return code
af_asyncts : avoid overflow in out_size with large delta values
ac3enc : simplify sym_quant ()
codec_desc : mark fraps as an intra - only codec .
avutil / opt : initialize ret
Arrays where one element too small , fixes CID114 .
avformat / mpeg : zero initialize idx_pkt
mpeg12 : terminate mpeg2_video_profiles arrays
lavfi / nnedi : Fix a memleak .
opt : check memory allocation
graphparser : add a NULL check on the argument passed to strstr
avcodec / vqavideo : use av_mallocz () for decode_buffer ()
avformat / takdec : zero padding area before use
avcodec / wnv1 : More strict buffer size check
h264 : fix clang warning about uninitialized variable
lavfi / gradfun : avoid use of uninitialized variable .
mss3 : prevent AC state from becoming invalid in rac_normalise ()
avformat / movenc : Fix potential leak of sgpd_entries array .
lzw ( gif ): Fix overread
lavc / mjpegdec : Do not overread too short JFIF tags .
h264 : fix () placement
aacenc : Prevent premature termination of the two loop search .
avcodec / cavsdec : Check esc_code
dfa : Put our pointer check back .
avoid deprecation warnings for AVFrac from avformat . h
error concealment : check last pictures motion_val before using it .
avcodec / mpegvideo_enc : Check for integer overflow in ff_mpv_reallocate_putbitbuffer ()
lavc / jpeg2000dec : Fix jp2 inner atom size used for overread checks .
avformat / tls_schannel : Fix use of uninitialized variable
ffmpeg : Fix null pointer dereference in do_video_out ()
dvenc : dont fail hard if the timecode is invalid
softfloat : handle - INT_MAX correctly
threads : fix a potential race spotted by helgrind .
avcodec / ituh263dec : Check for the bitstream end in ff_h263_decode_mb ()
avcodec / utvideodec : Fix bytes left check in decode_frame ()
Fix a leak in the AAC encoder
lavc / ass_split : check for NULL pointer in ff_ass_split_override_codes ().
wma : Clip WMA1 and WMA2 frame length to 11 bits .
avcodec / h264_refs : Change default case to av_assert0 () to suppress warning
avformat / mvdec : Fix DoS due to lack of eof check
gifdec : fix invalid write in gif_copy_img_rect
avcodec / shorten : if allocation fails reset max_frame_size
avcodec / vc1dec : fix null pointer dereference
lavfi / tinterlace : set inlink -> cur to NULL , since it is stored internally
vsrc_movie : do not free avoption variables in uninit ()
matroska : honor error_recognition on unknown doctypes
ffv1enc : Fix memleak
doc / examples / decoder_targeted : Disable error concealment after 20 frames
a64multienc : fix " warning : buf may be used uninitialized in this function "
lavfi / subtitles : 10l close codec before format .
avcodec / takdec : Fix runtime error : left shift of negative value - 42
avcodec / shorten : clear bitstream buffer
lagarith : check count before writing zeros .
h264 : don ' t re - call ff_h264_direct_ref_list_init () w / frame - mt .
avcodec / wavpackenc : fix uninitialized ret
avcodec / tiff : reset sampling [] if its invalid
mpegmux : add stuffing to avoid incomplete PCM frames
ffmpeg : add image size check to codec_get_buffer ()
quickdraw : Make the palette opaque
avcodec / dpx : fix framerate
motionpixels : clip VLC codes .
handle http error codes
fixed double free
doc / examples / transcoding : check encoder before using it
ac3enc : fix ' warning : block0 may be used uninitialized in this function '
flacdec : fix buffer size checking in get_metadata_size ()
avcodec / rangecoder : Test for invalid corner case
initialize mmcpos , found by guenther
oops , bounds_check_with_label is already proto ' d in sys / disklabel . h
Avoid an assertion failure in elf32_arm_size_dynamic_sections () when
rewrite construct that made GCC 2 . 8 do an implicit memset before libc
add LOG entry when opening feed in browser
target - i386 : Fix jmp im on x86_64 when executing 32 - bit code
s390x / skeys : Fix instance and class size
qemu - char : fix qemu_chr_fe_get_msgfd ()
linux - user : Check dump_write () return in elf_core_dump ()
linux - user : fix broken cpu_copy ()
loader : fix potential memory leak
target - i386 : fix protection bits in the TLB for SMEP
e1000e : Fix PBACLR implementation
qemu - img : mandate argument to ' qemu - img check -- repair '
exec : Fix bounce buffer allocation in address_space_map ()
block / qcow2 - bitmap : fix use of uninitialized pointer
fix off - by - one error in pci_piix3_xen_ide_unplug
block : Simplify usb_msd_initfn () test for " can read bdrv key "
exec - all . h : Make MAX_OP_PER_INSTR large enough for target - arm ' s uses
block / mirror : fix broken sparseness detection
msix : implement pba write ( but read - only )
scsi - disk : change disk serial length from 20 to 36
char : don ' t silently skip tn3270 protocol init when TLS is enabled
rdma : memory leak RDMAContext :: host
target - s390x : fix setcc in TCG mode
linux - user : Handle brk () attempts with very large sizes
vhost - user : fix watcher need be removed when vhost - user hotplug
target - ppc : Bug in BookE wait instruction
cirrus : don ' t overflow CirrusVGAState -> cirrus_bltbuf
esp : fix coding standards
s390x : reduce TARGET_PHYS_ADDR_SPACE_BITS to 62
rdma : Fix qemu crash when IPv6 address is used for migration
rdma : bugfix : ram_control_save_page ()
qemu - img : fix invalid JSON
ui / spice - display . c : add missing initialization for valgrind
qdev : don ' t crash on unset drive properties .
hw / sd . c : Set ILLEGAL_COMMAND for ACMDs in invalid state
block / parallels : fix access to not initialized memory in catalog_bitmap
cirrus : fix off - by - one in cirrus_bitblt_rop_bkwd_transp_ * _16
slirp : don ' t use " smb ports = 0 " option
hw / omap_gpio : Fix infinite recursion when doing 8 / 16 bit reads
qemu - ga : Plug fd leak on ga_channel_listen_accept () error path
ide : Fix off - by - one error in array index check
pci : Fix crash with illegal "- net nic , model = xxx " option
virtio - 9p : Fix syntax error in debug code
hw / lan9118 . c : Add missing ' break ' to fix buffer overrun
Qcow2 corruption fix , by Juergen Keil .
do_snapshot_blkdev () error on missing snapshot_file argument
target - alpha : fix decoding of CVTST / S
linux - user : assert that target_mprotect cannot fail
pc_sysfw : Plug memory leak on pc_fw_add_pflash_drv () error path
qapi : Add corrupt field to ImageInfoSpecificQCow2
char / serial : fix copy & paste error ( fifo8_is_full vs empty )
cryptodev : add xts ( aes ) support
Add a path length check to prevent heap overflow ( Eric Milliken ).
scsi - block : fix direction of BYTCHK test for VERIFY commands
block : Fix memory leaks in bdrv_co_do_pwritev ()
xhci : fix numintrs sanity checks
qemu - char : initialize chr_write_lock
usb - storage : Drop not needed " allow_hotplug = 0 "
pci - assign : Fix potential read beyond buffer on - EBUSY
i6300esb : fix timer overflow
target - arm : fix incorrect temporary variable freeing
net : make iov_to_buf take right size argument in nc_sendv_compat ()
dump : fix memory_mapping_filter leak
lm32_uart : fix receive buffering
dma : xlnx - zynq - devcfg : Fix up XLNX_ZYNQ_DEVCFG_R_MAX
qcow2 : Assert that the crypto header does not overlap other metadata
vhost - scsi : add missing virtqueue_size parameter
qdev : Fix crash when using non - device class name on - global
cadence_gem : avoid stack - writing buffer - overrun
gtk : fix possible memory leak about local_err
exec : avoid possible overwriting of mmaped area in qemu_ram_remap
qemu - char : fix warning ' res ' may be used uninitialized
linux - user / mmap . c : Avoid choosing NULL as start address
linux - user / syscall . c : remove warning : ‘ array ’ may be used uninitialized in this function
coroutine - sigaltstack : Change jmp_buf to sigjmp_buf
crypto : fix mistaken setting of Error in success code path
apic : kvm - apic : Fix crash due to access to freed memory region
qcow2 : Fix refcount table size calculation
ivshmem : check shm isn ' t already initialized
tcg : correct 32 - bit tcg_gen_ld8s_i64 sign - extension
virtio : optimize virtio_access_is_big_endian () for little - endian targets
pc - dimm : don ' t assert if pc - dimm alignment != hotpluggable mem range size
ui / console : fix OVERFLOW_BEFORE_WIDEN
console : Fix warning from clang ( and potential crash )
cpu - exec : Also reload CPUClass * cc after longjmp return in cpu_exec ()
sheepdog : fix NULL dereference in sd_create
io : remove mistaken call to object_ref on QTask
vhost - net : do not crash if backend is not present
exec : always use MADV_DONTFORK
Add a pc - 0 . 11 machine type and make the pc type an alias
vhost : fix a memory leak
qemu - img : add check for zero - length job len
spapr : sanity check size of the CAS buffer
qemu - option : Fix uninitialized value in append_option_parameter
qcow2 : fix offset overflow in qcow2_alloc_clusters_at ()
exit if - drive specified is invalid instead of ignoring the " wrong " - drive
cpu : don ' t allow negative core id
audio : fix WAVState leak
spapr_drc : Fix potential undefined behaviour
pci : Clean up error checking in pci_add_capability ()
exec . c : fix dirty bitmap reallocation
usb - ehci : drop assert ()
arm_gicv3 : Fix broken logic in ELRSR calculation
vga : check the validation of memory addr when draw text
vnc : Fix crash on ' info vnc ' after ' change vnc none '
target - i386 : Don ' t strdup () alias property name
qcow2 : Check min_size in qcow2_grow_l1_table ()
qdev : fix thinko leading to guest crashes
i386 ROR r8 / r16 instruction fix
fix qemu - system - unicore32 crashing when calling without - kernel
pci - hotplug : initialize dinfo to NULL in pci_device_hot_add
qapi : fix memory leak in bdrv_image_info_specific_dump
pcie : fix improper use of negative value
qcow2 corruption : Fix alloc_cluster_link_l2 ( Kevin Wolf )
qemu - sockets : Fix potential memory leak
rtl8139 : check TCP Data Offset field ( CVE - 2015 - 5165 )
xen / pt : xen_host_pci_config_read returns - errno , not - 1 on failure
virtio : add missing mb () on enable notification
usb - host : Initialize dev -> port the obviously safe way
target - i386 : CPUID : return highest basic leaf if eax > cpuid_xlevel
sheepdog : Fix misleading error messages in sd_snapshot_create ()
vnc : avoid possible file handler leak
pseries : Fix array overrun bug in PCI code
blockjobs : fix set - speed kick
spapr : fix write - past - end - of - array error in cpu core device init code
tap - win32 : skip unexpected nodes during registry enumeration
block / qcow . c : Fix memory leak in qcow_create ()
target - arm : Reinsert missing return statement in ARM mode SRS decode
hw / pci : fix crash on shpc error flow
cpu : Don ' t clear cpu -> exit_request on reset
vl : exit qemu on guest panic if - no - shutdown is not set
sd : sdhci : check data length during dma_memory_read
slirp : fix leak
pci : Fix memory leak
vga : Silence bogus gcc warning about uninitialized variables
vmdk : fix buf leak in vmdk_parse_extents ()
hw / arm / omap_gpmc : Avoid buffer overrun filling prefetch FIFO
sd : do not add one sector to the disk size
vvfat : fix out of bounds array_get usage
core / graph : free RGraphEdges when freeing long_edges list
Fix a gcc warning
logging : use fchmod instead of chmod to avoid TOCTOU race
Try to fix bug # 8885 ( negative gold amount gets changed while save / load )
fixed bug where the program could freeze up when networked
fixup ! Add DEPRECATED macro and remove uses of snprintf
fix possible OOB write in gif2tiff . c
* tools / tiffcrop . c : fix readContigStripsIntoBuffer () in - i ( ignore ) mode so
sd - dhcp - server : fix a leak
sd - login . c : fixup for d70964d0
seccomp : add proper help string for @ resources seccomp filter set
terminal : discard async read () errors for evdev
util : cunescape - fix memleak in error path
systemctl : fix check for template units
locale : make sure that l is freed
json : avoid cleanup of unitialized variable
sd - bus : correct size calculation in DBus fd receive
libudev : make valgrind happy
saEvtEventAttributesSet needs to check that the priority value passed in
cleanup valgrind warnings .
make it compile with - ansi - pedantic
[ t : 5131 ] closes # 5131 .
Tabname defaults to the table name for MONGO tables
branches / zip : buf_page_init_low (): Initialize also flush_type .
IB : production compilation fix
branches / zip : row_merge_create_temporary_table (): On error , free the
do not ignore first char , when truncating spaces in str -> num conversion .
sco compile fix : MAXPATHLEN
Merge a patch from MySQL AB ( Mats Kindal ):
Fix a double - free in rend_config_services ()
Fix a NULL pointer dereference in parse_server_transport_line ().
Generate our x509 certificates using sha256 , not sha1 .
r7341 @ Kushana : nickm | 2006 - 08 - 11 00 : 51 : 05 - 0700
fix ECP curve BER decode
fix erasepage on linux
move slurm_crypto_fini () to prevent memory leak on shutdown
Decrease the time of batch start time no more than DEFAULT_MSG_TIMEOUT .
fix extra space
fixed small leak in msRemoveStyle
Detach the current thread as soon as possible when mono_gc_pthread_exit is called .
Ignore seq points of entry and exit IL offsets .
[ mono ] Fix maybe - uninitialized warning .
Initialize d_result to NULL on ctor
queue : win32 : fix symbol export for M_queue_exists ()
link : plug a memory leak
eel - canvas : ignore extra mouse button events
fixed # 1783 ( fuzz )
Fixed crash introduced by trafID checks patch
fix XML dump when file does not have tracks ( e . g . HEIF files )
Prevent leaking memory from PIV readers
Made sure attribute CKA_TOKEN is set to TRUE .
coolkey : Make sure the object ID is unique when filling list
libopensc : initialize ' sc_ef_atr ' type variable
ext_mbstring : Fix invalid free () in php_mb_parse_encoding
Recognize the -- coerce xrdcp option ( it was forgotten ).
chanserv / antiflood : update mqueue_t . last_used when adding new messages
browser : check window liveness before setting title
lib , stream : fix stream sanity checks
ospf6d : fix off - by - one on display of spf reasons
bgpd : prevent double address delete on shutdown
regexp_release : allow NULL regexp
allow std :: string in setContentType as before
reset stop flag before starting threads
Fixed dereference to null pointer in ' codegen . c ' reported by ' clang - analyzer '
reduce invoking mrb_intern from convert_type
pluto : dpdaction = restart folloup to 24f14ba85ed250b
set the received message to zero , to make sure
fix typo . the previous commit brome make .
" Memory leak . Dynamic memory stored in ' file_content . buf ' allocated
git - svn - id : svn :// cherokee - project . com / cherokee / trunk @ 474 5dc97367 - 97f1 - 0310 - 9951 - d761b3857238
Bionic : Aliasing problems with frexpf
Fixed a memory leak in FileRef ' s OGA format detection
Fixed new bug due to a typo in the PNG00 fix
Moved check for https :// github . com / ImageMagick / ImageMagick / issues / 92 .
Added missing call to ConstrainColormapIndex .
Fixed head buffer overflow reported in : https :// github . com / ImageMagick / ImageMagick / issues / 98
Add another trace point .
fixed a warning in client ( mac )
fix another memory leak
* FileCopy . cc : don ' t create target file if source does not exist .
Fixed memory leak
sunos : filter out non - IPv4 / IPv6 interfaces
X - E3 to camera list
Duda : SQLite : fix return value on sql_dump ()
iov : buf to free non - zero allocation
shell - global : Use G_VARIANT_TYPE macro for checking
ssh : fix out of bounds read in banner parsing
cuda : fix invalid use of sizeof
rt : Fix size miscalculation in ivec_reserve
add transition helper to format . h
Fix the build .
removed silly check for >= 0 of a supposedly unsigned value !
hostip . c : return immediately from Curl_resolv_timeout () upon expired timeout .
lib / http : add missing whitespace in verbose output
http2 : abort the send_callback if not setup yet
curl_easy_reset : Also reset the authentication state
m_capab : fix a possible remote crash triggered by the CAPAB parsing code .
cmd / 8g : fix possibly uninitialized variable in foptoas .
fix bug introduced in 33293
cmd / ld : change DWARF encoding for array lengths
runtime : fix 386 , arm build
runtime : don ' t stop bitmap dump at BitsDead
Fix a crash in the asn1parse command
Added error checking for OBJ_create
pkey_gost_ctrl : check for NULL after allocating pctx -> shared_ukm
Remove an unused variable assignment
GH1141 : Different fix , preferred by Richard .
capi_get_key : check for NULL after allocating key
Bugfix : Free filename entry for writing config file when done with it
Put a lower limit on the nonce increment in cpu mining .
gif_read : Set last_name = NULL unconditionally .
Gifsicle : Free Clp_Parser before exit to clean valgrind report .
sestatus : free process and file contexts which are checked
fixed potential uninitialzed var access ( highly improbable )
Skip 0 - length ACL fields
Make a string translatable .
fix crash in limit when user enters invalid regexp
Refresh the log window on pool failure message at startup .
ccpp : open file for dump_fd_info with O_EXCL
refine addr_header comparison
fail on parse error
backlinks should be updated on rebind
add missing dd_close () to save_dump_dir_from_problem_data ()
fix minor textbuffer leak , Bug 288 by Toby
print - dhcp6 . c : add TCHECK (* dh6o ) in dhcp6opt_print ()
make pigz work on . spl files , otherwise pigz can ' t breaks with kiwi
tag server dirty if changing vars
destroy image and set MemoryError on overflow check failure
Fixing compile error from MSVC . Try 2 .
tpm2_getmanufec : only attempt to persist the EK if a handle was provided
mips : sanity check for input code length of Mips64
fix wrong function name in merge of GPR # 596
rewind wpa file descriptor when uploading
rgw : fix subuser key name when purging subuser keys
msg : MDentryLink has a private destructor
msgr : fix operator << for sockaddr_in , too
PG : don ' t compare auth with itself
fragtree_t : fix get_leaves_under ()
mds : properly free CInode :: ValidationContinuation
rados : statically allocate time
osd : clear INCONSISTENT if scrub detects no errors
kclient : check for ENAMETOOLONG in lookup
mds : choose reasonable state after xlock finishes
PG : set role for replicated even if role != shard
mon : crush straw_calc_version value is 0 or 1 not 0 to 2 .
add stray_prior_path to store path before rename
rgw : fix use - after - free
rgw : was missing return value
auth : fix NULL pointer access when trying to delete CryptoAESKeyHandler instance
osd : EINVAL when rmw_flags is 0
Rgw : check the return value when call fe -> run ()
auth : delete Exception once it ' s caught ; fixing minor memory leak
mds : zero out the layout in handle_client_setlayout
Changed name extension of calendar files from " vcf " to " ics ".
fix bit - length check in der_decode_raw_bit_string ()
EbmlElement : don ' t read beyond end of buffer when reading variable length integers
fixed firejail symlink problem for -- private - bin option
rsvg_characters_impl (): Unref the chars node we just created
Initialize pointer to NULL
squash heap - buffer - overflow , PoC TIFF from Hardik
virDomainGetTime : Deny on RO connections
ofproto : Fix OVS crash when reverting old flows in bundle commit
Throw on skipping an invalid type .
branches / zip : backport r3848 from 6 . 0 branch
reverting a patch
correct bug in code
branches / zip : buf_page_init_for_read (): Increment the buf_fix_count
fix github issue # 127
add NULL ctx argument check to wolfSSL_X509_STORE_CTX_set_time ()
initialize extra name entries to 0
[ ARM ] export copy_page
xen / xenbus : fix overflow check in xenbus_file_write ()
ath6kl : Release the memory allocated for the firmware
xilinx_emaclite : Call dev_consume_skb_any instead of dev_kfree_skb .
drivers / net / skfp : if ! capable ( CAP_NET_ADMIN ): inverted logic
[ PATCH ] auerswald . c : % zd for size_t
omap : 4430SDP : Register the card detect GPIO properly
staging : clocking - wizard : check for a valid clk_name pointer
gpio : palmas : fix a possible NULL dereference
staging : comedi : pcmuio : fix the last > 80 char line warning
ext4 : let ext4_group_add_blocks () handle 0 blocks quickly
[ PATCH ] fix Bug 4395 : modprobe bttv freezes the computer
dma : of - dma . c : fix memory leakage
[ ALSA ] Fix possible deadlocks in sequencer at removal of ports
ext3 : update ctime when changing the file ' s permission by setfacl
libceph : Remove spurious kunmap () of the zero page
ext4 : online defrag is not supported for journaled files
[ IA64 ] Correct pernodesize calculation .
cxlflash : off by one bug in cxlflash_show_port_status ()
[ ARM ] 5072 / 1 : corgi_pm : don ' t register devices on non - corgi machines
[ PATCH ] pcmcia : socket . functions starts with 1
ath10k : add module parameter values to the pci info print
Input : imx6ul_tsc - check for negative return value
ath6kl : fix memory leak in ath6kl_fwlog_block_read ()
mm : Adjust do_pages_stat () so gcc can see copy_from_user () is safe
brcmfmac : remove ( ab ) use of NL80211_NUM_ACS
fuse : invalidate target of rename
pinctrl : sirf : Remove gpiochip on failure cases
[ SCSI ] qla2xxx : Don ' t register to legacy interrupt for ISP82xx .
x86 / mce : Fix CPU hotplug and suspend regression related to MCE
ARM : SPEAr : conditionalize l2x0 support
[ TG3 ]: Update version to 3 . 82 .
iio : ti_am335x : Remove redundant call to iio_sw_buffer_preenable ()
tg3 : Disable 5717 serdes and B0 support
ecryptfs : avoid to access NULL pointer when write metadata in xattr
modedb : fix incorrect sync and vmode flags for CVT modes
spectra : Don ' t overwrite nand_debug_level . It ' s a module param
KVM : SVM : Set the ' busy ' flag of the TR selector
iio : iio : Fix iio_channel_read return if channel havn ' t info
ath9k : Send AUTHORIZED event only for station mode
[ S390 ] dasd : Use default recovery for SNSS requests
mv643xx_eth : prevent interrupt storm on ifconfig down
[ PATCH ] enable booting a NUMA system where some nodes have no memory
dlm : avoid unnecessary search in search_rsb
ncpfs : successful rename () should invalidate caches for parents
[ MIPS ] Fix return type of gt64120_irq .
clk : tegra : Use definition for pll_u override bit
ALSA : hda - Don ' t actually write registers for caps overwrites
Staging : vt6655 : add missing free_netdev () on error in hostap_enable_hostapd ()
x86 , vm86 : clean up invalid_vm86_irq ()
lib : correct 842 decompress for 32 bit
stmmac : fix max coal timer parameter
pasemi_mac : Access iph -> tot_len with correct endianness
drm / i915 : Wrap - EIO send - vblank event for failed pageflip in spinlock
sdhci : don ' t warn about sdhci 2 . 0 controllers
firewire : fix ioctl () return code
[ media ] r820t : better report signal strength
UIO : uio_pdrv : fix memory leak
i810fb : the pseudo_palette is only 16 elements long
[ PATCH ] i386 : Terminate backtrace fallback early if unwinder stack pointer is zero
ARM : dma - api : fix off - by - one error in __dma_supported ()
thinkpad - acpi : restrict procfs count value to sane upper limit
drm / i915 : fix lane bandwidth capping for DP 1 . 2 sinks
ath5k : add missing include for debug code
twl - regulator : Fix reg_disable functionality for 4030 and 6030
wan : wanxl : add pci_disable_device in case of error
igb : reset PHY after recovering from PHY power down
scm : Capture the full credentials of the scm sender
drm / nouveau : fix crash with noaccel = 1
drivers / net / usb / usbnet . c : Use FIELD_SIZEOF macro in usbnet_init () function .
perf stat : Using init_stats instead of memset
af_iucv : support ancillary data with HS transport
drm : missing drm_vm . c changes for consistent maps
[ XFS ] Wait for all I / O on truncate to zero file size
usb : message : Fixed parenthesis error in sizeof function .
xen / xenbus : export xenbus_dev_changed
vfs : fix spinning prevention in prune_icache_sb
kconfig : initialize the screen before using curses ( 3 ) functions
sctp : fix possible seqlock seadlock in sctp_packet_transmit ()
ASoC : core : Fix dai_link dereference .
thermal : return an error on failure to register thermal class
perf annotate browser : Fix behaviour of Shift - Tab with nothing focussed
f2fs : don ' t need to sync node page at every time
[ ALSA ] Fix substream to check in PCM drain
drm : Fix DRM_FORCE_ON_DIGITAL use
btrfs : quota : Update quota tree after qgroup relationship change .
ath10k : free wmi mgmt event skb when parsing fails
drivers / rtc / rtc - da9052 . c : register ability of alarm to wake device from suspend
r6040 : use ETH_ZLEN instead of MISR for SKB length checking
phy : fix phy address bug
i40evf : Add more info to interrupt vector names
pata_octeon_cf : fix broken build
ACPICA : Fix to allow aliases within ASL namepaths
drm : crtc : integer overflow in drm_property_create_blob ()
ata : fix sparse warning in libata . h
Btrfs : fix crash when starting transaction
ASoC : Intel : mfld - pcm : don ' t call trigger ops to DSP for internal streams
ARM : footbridge : fix overlapping PCI mappings
ssb : Fix error when V1 SPROM extraction is forced
drm / i915 : Initialise g4x watermarks for disabled pipes
KVM : Really remove a slot when a user ask us so
sys_sync (): fix 16 % performance regression in ffsb create_4k test
ARM : prevent VM_GROWSDOWN mmaps extending below FIRST_USER_ADDRESS
ath5k : validate mode in ath5k_ani_init () before trying to set it
perf evsel : Fix uninitialized memory access to struct perf_sample
xprtrdma : Back off rkey when FAST_REG_MR fails
ehca : convert cpu notifier to return encapsulate errno value
mac80211 : fix rx -> key NULL pointer dereference in promiscuous mode
USB : cyberjack : fix buggy integer overflow test
crypto : scatterwalk - Avoid flush_dcache_page on slab pages
mtd : mtd_blkdevs : don ' t increase ' open ' count on error path
drm / radeon : don ' t allow device to be opened if powered down
i2o : fix overflow of copy_to_user ()
ipv4 : fix nexthop attlen check in fib_nh_match
[ Bug 760 ] Fix setting WEP key in monitor mode causes IV lost .
mwl8k : fix NULL pointer dereference on receive out - of - memory
tg3 : 5785 : Set port mode to MII when link down
timer : Convert kmalloc_node (... GFP_ZERO ...) to kzalloc_node (...)
drm / i915 : The sprite scaler on Ironlake also support YUV planes
[ IA64 ] Hole in IA64 TLB flushing from system threads
[ PATCH ] moxa serial : add proper capability check
drm / exynos : check crtc ' s dpms mode at SetCrtc
[ media ] dvb_demux : fix deadlock in dmx_section_feed_release_filter ()
USB : serial : add special case for processing of empty read urbs
[ SCSI ] iscsi_boot_sysfs : Fix a memory leak in iscsi_boot_destroy_kset ()
drm / i915 : mark display port DPMS state as ' ON ' when enabling output
sound / soc / fsl / p1022_ds . c : add missing of_node_put
UBI : Fastmap : Notify user in case of an ubi_update_fastmap () failure
x86 / boot - image : Don ' t leak phdrs in arch / x86 / boot / compressed / misc . c :: Parse_elf ()
suspend : switch the Asus Pundit P1 - AH2 to old ACPI sleep ordering
drm / i915 : Permit pinning whilst the device is ' suspended '
[ SCSI ] be2iscsi : fix bad if expression
gianfar : Check if phydev present on ethtool - A
mm : migrate : hugetlb : putback destination hugepage to active list
usb : phy : mv - otg : use to_delayed_work instead of cast
ieee1394 : sbp2 : prepare for s / g chaining
radeon_display : Use pointer return error codes
perfcounters : fix refcounting bug
iwlwifi : mvm : don ' t allow sched scans without matches to be started
dm era : check for a non - NULL metadata object before closing it
befs : fix leak
[ SCSI ] bnx2fc : Drop incoming ABTS
mfd : sec - core : Enable RTC on S2MPS11 PMIC
[ PATCH ] scsi : device_reprobe () can fail
dmaengine : i . MX : Fix merge of cookie branch .
uprobes : Allocate -> utask before handler_chain () for tracing handlers
xfs : prevent leaking uninitialized stack memory in FSGEOMETRY_V1
kobject : free memory if netlink_kernel_create () fails
drm / i915 : mark a newly - created GEM object dirty when filled with data
irqchip : irq - crossbar : Not allocating enough memory
drm / radeon / kms : set fbdev_info for suspend / resume
drm / i915 : fixup assert_pipe to take the pipe A quirk into account
hfsplus : use - ENOMEM when kzalloc () fails
tpm_tis : free irq after probing
drm / mgag200 : free memory allocated with alloc_apertures ()
i2c : designware : retry transfer on transient failure
ext4 : add missing kfree () on error return path in add_new_gdb ()
USB : musb : forward debug mode feature to gadget
tty : xuartps : Fix " may be used uninitialized " build warning
drm / i915 / sdvo : Cleanup connector on error path
perf kmem : Fix compiler warning about may be accessing uninitialized variable
dmaengine : sirf : off by one in of_dma_sirfsoc_xlate ()
ASoC : rt5640 : add default case for unexpected ID
staging : unisys : fix spacing in parahotplug_request_create
[ PATCH ] bcm43xx : Fix deviation from specifications in set_baseband_attenuation
Staging : vt6656 : fix the memory free bug in vntwusb_disconnect ()
ALSA : pcm : optimize xrun detection in no - period - wakeup mode
cgroup : fix an off - by - one bug which may trigger BUG_ON ()
leds : leds - cobalt - raq . c - use resource_size ()
ipmi : Clear drvdata when interface is removed
ath9k : skip ATH9K_INT_TIM_TIMER when we are idle
staging / zcache : Fix memory leak while dropping an o2net_sock_container object
ocfs2 : check if cluster name exists before deref
drm / nouveau : remove pointless assignment
Input : turbografx - clear unused function pointers
ath9k : Remove unused rx_edma in ath_rx_addbuffer_edma ()
vm : fix incorrect unlock error path in madvise_free_huge_pmd
ath9k : No need for that extra memcpy
ALSA : hda - Fix NULL dereference with CONFIG_SND_DYNAMIC_MINORS = n
staging : mt29f_spinand : Export OF module alias information
perf tools : Add missing closedir in multi tracepoint processing
KVM : destroy workqueue on kvm_create_pit () failures
pinctrl : pinctrl - imx1 - core . c : Cleaning up if unsigned is less than zero
[ PATCH ] PCI Hotplug : shpchp driver doesn ' t program _HPP values properly
Btrfs : fix scheduler warning when syncing log
m68k : export atari_keyb_init
powerpc : kmalloc failure ignored in vio_build_iommu_table ()
x86 : fix warning on 32 - bit non - PAE
mac80211 : tx , use dev_kfree_skb_any for beacon_get
perf : Fix the software events state check
[ PATCH ] fix creating zero sized bio mempools in low memory system
parisc : isa - eeprom - Fix loff_t usage
sctp : disable max_burst when the max_burst is 0
[ PATCH ] hrtimer : validate timespec of do_sys_settimeofday
ext4 : don ' t leave i_crtime . tv_sec uninitialized
ALSA : pcm : fix delta calculation at boundary wraparound
staging / slicoss : disable pci device at remove
mt7601u : don ' t warn about devices without per - rate power table
staging : iio : Documentation : rewrite the right hand side of an assignment
xfs : catch invalid negative blknos in _xfs_buf_find ()
[ PATCH ] spufs fix spu_acquire_runnable error path
drm / nvc0 / ltcg : mask off intr 0x10
mac80211 : drop 4 - addr VLAN frames earlier if not connected
[ media ] cx25821 : do not expose broken video output streams
[ media ] dvb_frontend : fix compiler warning
drivers / scsi / aacraid / commctrl . c : fix mem leak in aac_send_raw_srb ()
USB : pl2303 : fix return value of tiocmset
xfs : ensure we capture IO errors correctly
ipv6 : Fix ip6_xmit to send fragments if ipfragok is true
ALSA : fix excessive background noise introduced by OSS emulation rate shrink
zd1211rw : use unaligned safe memcmp () in - place of compare_ether_addr ()
[ PATCH ] ppc32 : don ' t call progress functions after boot
[ media ] DVB : Set scanmask for Budget / SAA7146 cards
bpf : avoid copying junk bytes in bpf_get_current_comm ()
[ SCSI ] remove unnecessary scsi_delete_timer () call in scsi_reset_provider ()
xfs : fix memory leak in xlog_recover_add_to_trans
i2c : i2c - pnx : Limit minimum jiffie timeout to 2
at86rf230 : add force slotted operation bit
mac80211 : initialize sta -> last_rx in sta_info_alloc
block : move timeout field in struct request to pack better
[ PATCH ] Enable RAID autorun on Mac partition tables .
[ SCSI ] qla2xxx : Ensure RISC - interrupt - enabled consistency for IS_NOPOLLING_TYPE () ISPs .
proc / sysctl : fix the int overflow for jiffies conversion
kconfig / nconf : prevent segfault on empty menu
pch_uart : add spin_lock_init
UDF : Fix a null pointer dereference in udf_sb_free_partitions
Btrfs : set truncate block rsv ' s size
airo : return from set_wep_key () when key length is zero
proc : fix the potential use - after - free in first_tid ()
Btrfs : fix regression of btrfs device replace
mmc : dw_mmc : fix the wrong condition checking
cpufreq : fix obvious condition statement error
HID : extend mask for BUTTON usage page
ext4 : online defrag is not supported for journaled files
JFS : FIx one more plain integer as NULL pointer warning
igb : limit EEPROM access
mmc : unbreak sdhci - esdhc - imx on i . MX25
spi : fsl : Use kmalloc_array () in of_fsl_spi_get_chipselects ()
ASoC : ml26124 : Fix negative array index read
mac80211 : fix TX aggregation TID struct leak
Btrfs : avoid memory leak of extent state in error handling routine
xen / gntalloc : release grant references on page free
gpio : f7188x : fix edit mistake
acm : Return ENODEV instead of EINVAL when trying to open ACM device .
fsl / fman : double free on probe failure
ath9k : Remove pointless ath9k_ps_restore () in ath_detach ()
[ media ] radio - bcm2048 : fix missing unlock on error in bcm2048_rds_fifo_receive ()
drm / radeon / si : add dpm quirk for Oland
ASoC : Serialize access to dapm_power_widgets ()
greybus : core : fix hd - creation error path
cxlflash : Fix to prevent stale AFU RRQ
bonding : in balance - rr mode , set curr_active_slave only if it is up
ipv6 : ignore null_entry in inet6_rtm_getroute () too
bnx2x : correct advertisement of pause capabilities
sched / headers : Remove the runqueue_is_locked () prototype
staging : r8188eu : Add files for new drive : Cocci spatch " noderef "
hfsplus : handle match_strdup failure
HID : multitouch : force retrieving of Win8 signature blob
s390 / chsc : fix ioctl CHSC_INFO_CU command
mfd : Use devm_ * APIs for mc13xxx : remove leftover kfree
ipvs : info leak in __ip_vs_get_dest_entries ()
drm / radeon : force MSIs on RS690 asics
[ PATCH ] Add deprecated_for_modules
powerpc / pci : fix PCI - e check link issue
[ media ] dvb_usb_v2 : fix error handling for . tuner_attach ()
KVM : x86 emulator : refuse SrcMemFAddr ( e . g . LDS ) with register operand
usb : dwc3 : gadget : stop touching HWO TRBs
ARM : OMAP : board - omap4panda : MUX configuration for sys_nirq2
usb : gadget : dummy_hcd : clear usb_gadget region before registration
fs : Fix theoretical division by 0 in super_cache_scan ().
i2c - cpm : drop NO_IRQ
pNFS : Skip invalid stateids when doing a bulk destroy
drm / nouveau / bios / mxm : handle digital connector table 1 . 1
ceph : fscache : Update object store limit after file writing
USB : serial : fix potential heap buffer overflow
USB : xhci : drop spinlock in xhci_urb_enqueue () error path .
drm : Avoid oops in GEM execbuffers with bad arguments .
PCI : Do not treat EPROBE_DEFER as device attach failure
Input : ads7846 - avoid pen up / down when reading hwmon
greybus : timesync / pm : Make synchronous call to restore FrameTime
iwlwifi : range checking issue
radiotap : fix bitmap - end - finding buffer overrun
ASoC : DaVinci : pcm , constrain buffer size to multiple of period
drm / radeon : fix endian bug in radeon_atom_get_mclk_range_table ()
udf : Fix possible corruption when close races with write
cfq : choose a new next_req when a request is dispatched
dmaengine : st_fdma : Fix the error return code in st_fdma_probe ()
[ media ] DaVinci - VPFE - Capture : Replace a memcpy () call by an assignment in vpfe_enum_input ()
ieee802154 : Fix oops during ieee802154_sock_ioctl
Btrfs : skip adding an acl attribute if we don ' t have to
ds2760_battery : Add missing kfree ( di ) in ds2760_battery_remove ()
[ PATCH ] mempolicy . c GFP fix
usb : xhci : plat : Enable async suspend / resume
[ IPV6 ]: Reorg struct ifmcaddr6 to save some bytes
ext4 : avoid exposure of stale data in ext4_punch_hole ()
[ libata ] sata_sil : fix used - uninit warning
[ NETFILTER ]: PPTP helper : fix sstate / cstate typo
xen : mask _PAGE_PCD from ptes
ieee1394 : sbp2 : fix probing of some DVD - ROM / RWs
proc : mem_release () should check mm != NULL
firewire : ohci : correct signedness of a local variable
[ SCSI ] fix two leaks in scsi_alloc_sdev failure paths
usb : r8a66597 : add locking to r8a66597_check_detect_child
USB : cdc - acm . c : Fix compile warnings
block : check partition alignment
Btrfs : check if previous transaction aborted to avoid fs corruption
staging : sm7xx : Fixup of the section mismatch
mfd : Check for twl4030 - madc NULL pointer
video : vt8500lcdfb : Remove kfree call since devm_kzalloc () is used
i2c / ds1374 : Check workqueue creation status
Btrfs : align offsets for btrfs_ordered_update_i_size
staging : greybus : make cport_quiesce () method optional
axp288_fuel_gauge : Add original author details
ASoC : si476x : Fix IO setup
batman - adv : Detect missing primaryif during tp_send as error
[ SCSI ] lpfc : introduce missing kfree
drbd : Null pointer deref fix to the large " multi bio rewrite "
gpio : remember to finally free gpio_device
staging : slicoss : fix a leak when kzalloc fail
tipc : check return value of nlmsg_new
drm / ttm : test for dma_address array allocation failure
btrfs : rw_devices shouldn ' t be incremented for seed fs in btrfs_rm_dev_replace_srcdev ()
[ media ] v4l2 - ctrls . c : add NULL check
usb : gadget : f_printer : actually limit the number of instances
spi : core : Fix check for dma_map_sg () failures
[ POWERPC ] spufs : check spu_acquire_runnable () return value
pinctrl : mediatek : add input - enable setting in gpio_request_enable
s390 / scm_blk : fix memleak in init function
ixgbe : remove ifdef check for non - existent define
x86 : Update UV3 hub revision ID
drm / i915 : Don ' t call sg_free_table () if sg_alloc_table () fails
rndis_wlan : add return value validation
drm / tegra : Use lockless gem BO free callback
staging : vt6655 : check ieee80211_bss_conf bssid not NULL
eeepc - wmi : set the right key code for 0xe9
geneve : Unregister pernet subsys on module unload .
bnx2x : Prevent " Unknown MF " print in SF mode
ceph : handle ceph_osdc_new_request failure in ceph_writepages_start
xen / balloon : Don ' t continue ballooning when BP_ECANCELED is encountered
mac80211 : Change WARN_ON to WARN_ON_ONCE
ceph : reset iov_len when discarding cap release messages
[ media ] ascot2e : Fix I2C message size check
staging / lustre / llite : Fix integer overflow in ll_fid2path
[ TCP ]: Clear stale pred_flags when snd_wnd changes
batman - adv : remove extra negation in gw_out_of_range ()
[ PATCH ] sata_sil24 : kill 10ms sleep in softreset
btrfs : remove unnecessary list_del
lpfc : Fix internal loopback failure .
gianfar : Do not reuse pages from emergency reserve
pktgen : fix crash with vlan and packet size less than 46
spi : spi - davinci : Fix variable type
staging : iio : Make write_event_value callback optional
virt / kvm / assigned - dev . c : Set ' dev -> irq_source_id ' to '- 1 ' after free it
[ ALSA ] trident - Shut up compile warnings
drm / amd / powerplay : fix memory leak of tdp_table
usbnet : smsc95xx : don ' t recover device if suspend fails in system sleep
[ SCSI ] isci : fix breakage caused by > 16byte CDB patch
PCI : of : Add 64 - bit address recognition without LPAE support
f_phonet : fix page offset of first received fragment
kobject : free memory if netlink_kernel_create () fails
xfs : silence uninitialised f . file warning .
[ SCSI ] nsp_cs : fix buf overflow
crypto : camellia_aesni_avx - Fix CPU feature checks
ASoC : rt5645 : Check if codec is initialized in workqueue handler
irq : check domain hwirq range for DT translate
squashfs : add missing buffer free
mm , vmstat : Remove spurious WARN () during zoneinfo print
[ PATCH ] savagefb : Remove NULL check
ALSA : ALC283 codec - Avoid pop noise on headphones during suspend / resume
[ PATCH ] libata : fix missing classes [] initialization in ata_bus_probe ()
blk - throttle : check stats_cpu before reading it from sysfs
sysctl : Drop reference added by grab_header in proc_sys_readdir
staging : usbip : userspace : Check return value for mkdir ()
i82875p_edac : fix overflow device resource setup
e1000 : fix WARN_ON with mac - vlan
drivers : usb : Mark function as static in usbsevseg . c
sysctl : Add neg_one as a standard constraint
x86 , amd - ucode : Check UCODE_MAGIC before loading the container file
i2c : mux : reg : Provide of_match_table
[ ALSA ] soc - Clean up tabs
slab numa fallback logic : Do not pass unfiltered flags to page allocator
USB : add a private - data pointer to struct usb_tt
drm / i915 : switch off backlight for backlight class 0 brightness
staging : comedi : das16 : use comedi_handle_events ()
input : pmic8xxx - pwrkey : Do not use mfd_get_data ()
iio : staging : ad7298_ring : Fix maybe - uninitialized warning
tracing : Reset parser -> buffer to allow multiple " puts "
Staging : comedi : kcomedilib : replace deprecated simple_strtoul () with kstrtouint ()
[ NETFILTER ]: recent match : missing refcnt initialization
drm / ttm : fix stupid parameter inversion in the pipeline code
bnx2x : Change to driver version 1 . 72 . 10 - 0
ARM : shmobile : armadillo800eva : set clock rates before timer init
i40e : Fix memory leak at failure path in i40e_dbg_command_write ()
bnx2x : fix possible panic under memory stress
ALSA : pcm : don ' t override timestamp unconditionally
usbnet : allow mini - drivers to consume L2 headers
iwlwifi : mvm : disable uAPSD due to bugs in the firmware
mfd : cros_ec : Instantiate ChromeOS EC character device
kernel / sys . c : fix missing rcu protection for sys_getpriority ()
mwiflex : avoid possible null pointer dereference
clk : mvebu : fix setting unwanted flags in CP110 gate clock
mailbox , pl320 - ipc : remove __init from probe function
cpufreq : intel_pstate : Fix setting max_perf_pct in performance policy
KVM : VMX : Only reload guest msrs if they are already loaded
drm : Return EBUSY if the framebuffer is unbound when flipping .
uml : drivers / net_user . c memory leak fix
KVM : x86 : Improve thread safety in pit
ASoC : imx - es8328 : Register jacks at the card level
drm / nouveau : v3 . 0 pll limits tables have type <-> register mapping too
perf tools : Fix map_groups__clone to put cloned map
perf / hwbp : Fix cleanup in case of kzalloc failure
[ media ] s5p - jpeg : remove stray call to readl
mac80211 : fix check for buffered powersave frames with txq
virtio : console : We support only one device at a time
cifs : always do is_path_accessible check in cifs_mount
KVM : s390 : kvm / sigp . c : fix memory leakage
cciss : remove unnecessary casts
ARM : 7772 / 1 : Fix missing flush_kernel_dcache_page () for noMMU
sysfs : sysfs_write_file () writes zero terminated data
staging : rtl8712 : Replace kmalloc with kmalloc_array
tipc : fix variable dereference before NULL check
staging : android : sync : Fix memory corruption in sync_timeline_signal ().
orinoco : fix TKIP countermeasure behaviour
Staging : hv : channel : export vmbus_close to modules
blk - stat : fix blk_stat_sum () if all samples are batched
USB : Remove unnecessary casts of void ptr returning alloc function return values
UBI : bugfix : dont oops with NULL module parameter
[ ALSA ] hdsp - Fix zero division
[ ARM ] 4233 / 1 : nand / s3c2410 . c : warning fix
ASoC : s6000 : Use WARN_ON () instead of BUG_ON ()
rxrpc : rxrpc_kernel_send_data () needs to handle failed call better
perf tools : Fix build breakage on arm64 targets
clk : at91 : Fix memory leak in of_at91_clk_master_setup ()
ARM : 5623 / 1 : Treo680 : ir shutdown typo fix
fix pgoff in " have to relocate " case of mremap ()
ixgbe : Allow FDB entries access to more RAR filters
KVM : PPC : Add emulation for dcba
cgroup : set ' start ' with the right value in cgroup_path .
ipv6 : exit early in addrconf_notify () if IPv6 is disabled
tcp_output : fix sparse warning for tcp_wfree
bridge : provide a mtu () method for fake_dst_ops
mfd : Pass htc - pasic3 led platform data through the cell platform_data
perf symbols : Properly align symbol_conf . priv_size
[ media ] media : davinci : vpif_display : drop unused member fbuffers
[ media ] s5h1420 : fix a buffer overflow when checking userspace params
dma : edma : Fix memory leak
dnet : Support the get_ts_info ethtool method .
Btrfs : init inode ordered_data_close flag properly
mmc : sdhci - bcm2835 : Clean up platform allocations if sdhci init fails .
drivers / net / wireless / b43 / main . c : 4351 : Fixed coding style
drm / i915 / gvt / kvmgt : remove some dead code
sfc : check for allocation failure
ALSA : hda - Update automute / automic upon jack retasking
ARM : ensure vm_struct has its phys_addr member filled in
coredump : make coredump_wait wait for mmap_sem for write killable
powerpc / powernv / ioda2 : Gracefully fail if too many TCE levels requested
tcp : zero ca_priv area when switching cc algorithms
ext4 : verify extent header depth
usb : dwc3 : omap : use the macro we already have
ASoC : simple - card : Add a NULL pointer check in asoc_simple_card_dai_link_of
ASoC : dpcm : Add checks of playback / capture before dpcm_get_be
[ PATCH ] USB : Fix typo in ohci - ppc - soc . c : usb_hcd_put => usb_put_hcd
atl1 : zero out CMB and SBM in atl1_free_ring_resources
[ media ] tveeprom : add support for Xceive XC5000C tuner
stmmac : fix oversized frame reception
tcp : fix 0 divide in __tcp_select_window ()
dm thin : fix a memory leak when passing discard bio down
[ MTD ] [ OneNAND ] fix call to onenand_verify when writing subpages
drm / radeon / si_dpm : Limit clocks on HD86xx part
audit : log module name on delete_module
cpuidle , menu : use interactivity_req to disable polling
xHCI : add XHCI_RESET_ON_RESUME quirk for VIA xHCI host
[ PATCH ] uml : activate_fd : return ENOMEM only when appropriate
ASoC : Intel : Skylake : Don ' t unload module when in use
ext4 : disable COLLAPSE_RANGE for bigalloc
perf / ftrace : Fix paranoid level for enabling function tracer
ceph : cleanup aborted requests when re - sending requests .
[ media ] usbvision : fix locking error
[ ALSA ] Fix possible deadlocks in sequencer at removal of ports
ext4 : fix error paths in swap_inode_boot_loader ()
tridentfb : use display information in info not in var for panning
[ media ] davinci : dm355 : Fix uninitialized variable compiler warnings
x86 : don ' t send SIGBUS for kernel page faults
memstick : init sysfs attributes
irda : smsc wait count reaches - 1
[ media ] hdpvr : Fix memory leak
phy : qcom - qusb2 : Fix crash if nvmem cell not specified
usb : storage : shuttle_usbat : fix discs being detected twice
ALSA : usb - audio : WARN_ON when alts is passed as NULL
drivers / usb / host / ehci - fsl . c : add missing iounmap
Input : ad7877 - filter events where pressure is beyond the maximum
staging / xgifb : Remove unnecessary binary calculation in GetXG27FPBits
USB : CDC ACM : Fix NULL pointer dereference
proc : fix oops on invalid / proc /< pid >/ maps access
[ PATCH ] struct path : convert ieee1394
[ S390 ] hw_random : allow rng_dev_read () to return hardware errors .
libata : ensure host is free ' d on error exit paths
staging : slicoss : fix a leak when kzalloc fail
ACPICA : Fix memory leak on unusual memory leak
drm / ttm : test for dma_address array allocation failure
ACPI : battery : Support for non - spec name for LiIon technology
bpf : ixgbe : Report bpf_prog ID during XDP_QUERY_PROG
drm / i915 : Clear crtc atomic flags at beginning of transaction
ext4 : fix incorrect block reservation on quota transfer .
drm / tilcdc : Take CRTC lock when calling tilcdc_crtc_disable ()
xfs : fix inobt inode allocation search optimization
[ media ] [ v3 , media ] av7110 : check for negative array offset
ALSA : hda - Add mic - boost controls to ALC662 / 663 auto configuration
[ PATCH ] orinoco : Fix memory leak on error in processing hostscan frames .
tipc : check nl sock before parsing nested attributes
f2fs : correct removexattr behavior for null valued extended attribute
libnvdimm , btt : check memory allocation failure
wl12xx : fix a memory leak of probereq template upon recovery
drm : i915 : Explicitly apply PWM config extracted from pwm_args
block : cciss : fix information leak to userland
KVM : arm / arm64 : timer : Check for properly initialized timer on init
KEYS : prevent KEYCTL_READ on negative key
ipv6 : don ' t use tw net when accounting for recycled tw
HID : corsair : fix mapping of non - keyboard usages
Staging : rtl8192e : Fix potential NULL pointer dereference
[ POWERPC ] spufs : Don ' t yield nosched context
drm / nv50 / disp : allow interlaced and doublescan modes on digital outputs
f2fs : avoid wrong error during recovery
mm : memcg : Correct unregistring of events attached to the same eventfd
ALSA : hdsp : allow firmware loading from inside the kernel
drm / amd / amdgpu : fix over - bound accessing in amdgpu_cs_wait_any_fence
ext4 : avoid hang when mounting non - journal filesystems with orphan list
perf symbols : Only un - prelink non - zero symbols
ALSA : ice1724 - Add PCI postint to reset sequence
[ PATCH ] ppc64 : iSeries early printk breakage
ASoC : Intel : Skylake : Initialize NHLT table
ipv6 : sr : fix out - of - bounds access in SRH validation
ALSA : hda - Make azx_attach_pcm_stream static
floppy : don ' t write kernel - only members to FDRAWCMD ioctl output
USB : ehci : fix debugfs ' lpm ' permissions
uwb : uwbd () is not freezable kthread
staging : unisys : visorbus : Check controlvm message payload size
usb : chipidea : Allow disabling streaming not only in udc mode
[ PATCH ] irq - devres : fix failure path of devm_request_irq ()
Input : twl4030_keypad - pass correct pointer to free_irq ()
[ PATCH ] s390 : chps [] array too short
sctp : better integer overflow check in sctp_auth_create_key ()
sctp : do not inherit ipv6_ { mc | ac | fl } _list from parent
drm / radeon / kms : Fix NULL ptr dereference
iwlwifi : dbg_ini : fix memory leak in alloc_sgtable
x86 : fix warning on 32 - bit non - PAE
i2c : xiic : Remove the Addressed as slave interrupt
loopback : off by one in tcm_loop_make_naa_tpg ()
scsi : sg : allocate with __GFP_ZERO in sg_build_indirect ()
mlxsw : spectrum_router : Fix error path in mlxsw_sp_router_init
igb : Fix a memory leak in igb_probe
mtip32xx : fix potential crash on SEC_ERASE_UNIT
sctp : support sysctl to allow users to use stream interleave
drivers : hamradio : 6pack : fix UAF bug caused by mod_timer ()
xfrm_user : validate XFRM_MSG_NEWAE XFRMA_REPLAY_ESN_VAL replay_window
signal : always clear sa_restorer on execve
quota : info leak in quota_getquota ()
rcuclassic : fix compiler warning
x86 : remove set_fixmap () warning
ax25 : Fix UAF bugs in ax25 timers
drm / ttm : fix memory leak noticed by kmemleak .
pcie port driver : correctly detect native PME feature
xfs : use -> t_dfops in log recovery intent processing
e1000e : do not attempt to disable smart powerdown if not supported
gpu : host1x : clk_round_rate () can return a zero upon error
perf tools : Do not accept parse_tag_value () overflow
batman - adv : Only write requested number of byte to user buffer
xfs : fix freeing memory in xfs_getbmap ()
perf / x86 / intel / uncore : Correct fixed counter index check for NHM
cpufreq : Do not clear real_cpus mask on policy init
xfs : fix missing error check in xfs_rtfree_range
leds - atmel - pwm . c : Make pwmled_probe () __devinit
[ media ] smsusb : fix the default delivery system setting
greybus : interface : fix potential attribute - buffer overflow
KVM : x86 : fix error return code in kvm_arch_vcpu_init ()
powerpc / mm : Fix hash table dump when memory is not contiguous
drm / sun4i : Fix uninitialized variables in vi layer
perf : arm64 : Record the user - mode PC in the call chain .
HID : picolcd : sanity check report size in raw_event () callback
mac80211 : properly free requested - but - not - started TX agg sessions
af_key : Do not call xfrm_probe_algs in parallel
staging : ozwpan : prevent overflow in oz_cdev_write ()
bpf : sockmap , duplicates release calls may NULL sk_prot
spi / ppc4xx : Use kcalloc () in spi_ppc4xx_of_probe ()
vfio / spapr_tce : Check kzalloc () return when preregistering memory
video : fbdev : pxa3xx - gcu : Fix integer overflow in pxa3xx_gcu_write
Btrfs : fix btrfs_search_slot_for_read backwards iteration
ALSA : HDA : Fix volume control naming for surround speakers on Realtek auto - parser
Btrfs : fix return value from btrfs_listxattr when buffer size is too small
jbd2 : preserve original nofs flag during journal restart
aoe : Fix OOPS after SKB queue changes .
fuse : destroy bdi on error
page_cgroup should ignore empty nodes
x86 : fix broken flush_tlb_others_ipi ()
rapidio / tsi721 : fix incorrect detection of address translation condition
ASoC : sst_platform : free the resources on fail path
mfd : palmas : Initialise client -> of_node for dummy created client
llc : better deal with too small mtu
KVM : x86 : fix NULL deref in vcpu_scan_ioapic
igb : Fix Null - pointer dereference in igb_reset_q_vector
dlm : close othercons
ALSA : timer - Fix Oops at closing slave timer
dccp : fix freeing skb too early for IPV6_RECVPKTINFO
staging : comedi : usbduxsigma : remove tidy_up () from (* auto_attach )
UDF : Close small mem leak in udf_find_entry ()
sched : Guarantee new group - entities always have weight
ACPI : Remove acpi_pci_slot_init () headers from internal . h
USB : ehci : add call of free_cached_itd_list () function in disable_periodic ()
Btrfs : fix wrong max_to_defrag in btrfs_defrag_file ()
nvmet - fc : ensure target queue id within range .
cifs : Silence uninitialized variable warning
drm / i915 : A hotfix for making aliasing PPGTT work for GVT - g
lightnvm : fail fast on passthrough commands
Input : don ' t modify the id of ioctl - provided ff effect on upload failure
scsi : qedi : Fix a potential buffer overflow
drm / mediatek : don ' t use drm_put_dev
orinoco : fix TKIP countermeasure behaviour
drm : clean dirty memory after device release
ALSA : timer : Fix leak in SNDRV_TIMER_IOCTL_PARAMS
ath9k_hw : fix synth delay for half / quarter channels
[ PATCH ] x86 - 64 : Fix C3 timer test
ALSA : usb - audio - clear chip -> probing on error exit
[ S390 ] add atomic64_xchg to s390
x86 / cpu , x86 / pti : Do not enable PTI on AMD processors
drm : fixes when i2c encoder slave mode_fixup is null .
ASoC : Intel : Fix incorrect sizeof () in sst_hsw_stream_get_volume ()
kvm : x86 : fix kvm_apic_has_events to check for NULL pointer
xfs : validate acl count
ASoC : Intel : Skylake : Fix the module state check condition
ARM : S5P64X0 : Replace irq_gc_ack () with irq_gc_ack_set_bit ()
wl1271 : Fix mac80211 RTS threshold requests during WL1271_STATE_OFF
sock_diag : Fix out - of - bounds access to sock_diag_handlers []
[ SCSI ] remove unnecessary scsi_delete_timer () call in scsi_reset_provider ()
scsi : fnic : Zero io_cmpl_skip on fw reset completion
ieee1394 : node manager causes up to ~ 3 . 25s delay in freezing tasks
fuse : fix SEEK_END incorrectness
clk : versatile : fix memory leak
mv643xx_eth : check for valid hw address ( resubmit )
CacheFiles : Fix incorrect test for in - memory object collision
xen - gntalloc : integer overflow in gntalloc_ioctl_alloc ()
arm / arm64 : KVM : Load the timer state when enabling the timer
[ PATCH ] aacraid : swapped kmalloc args .
ath9k_htc : Fix free slot value for cab queue
tick / nohz : Fix possible missing clock reprog after tick soft restart
perf / core : Fix the perf_cpu_time_max_percent check
hpfs : fix warnings when the filesystem fills up
arch / tile : check kmalloc () result
[ GFS2 ] gfs2 endianness bug : be16 assigned to be32 field
ARM : 6665 / 1 : Add Snapper9260 rtc wakeup interrupt pin
Driver Core : don ' t oops with unregistered driver in driver_find_device ()
ipmi : info leak in compat_ipmi_ioctl ()
tcm_loop : Fix memory leak in tcm_loop_submission_work error path
scsi : ufs : ufs - mediatek : Fix error checking in ufs_mtk_init_va09_pwr_ctrl ()
rxrpc : Make service connection lookup always check for retry
[ XFS ] Fix nused counter . It ' s currently getting set to - 1 rather than
perf inject : Do not repipe attributes to a perf . data file
KEYS : ensure we free the assoc array edit if edit is valid
ipvs : Reject ipv6 link - local addresses for destinations
ALSA : hda / realtek - Fix possible Oops with NULL input_mux
wil6210 : missing reinit_completion in HALP voting
pch_dma : fix error return code in pch_dma_probe ()
clk : make sure parent is not NULL in clk_fetch_parent_index ()
xen / event : validate irq before get evtchn by irq
KVM : nSVM : always intercept VMLOAD / VMSAVE when nested ( CVE - 2021 - 3656 )
generic - adc - battery : Fix checking return value of request_any_context_irq
nfsd : clean up reset_union_bmap_deny
RDS : verify the underlying transport exists before creating a connection
x86 : ptrace , bts : fix an unreachable statement
btrfs : memory leak in btrfs_add_inode_defrag ()
tcp : skb_mstamp_after helper
Staging : vt6656 : checkpatch : fix identifier name in wcmd . h
bpf : devmap fix arithmetic overflow in bitmap_size calculation
simple_xattr : permit 0 - size extended attributes
mtd : atmel_nand : increase chip_delay
signal : Fix name of SIGEMT in # if defined () check
tpm_crb : check for bad response size
staging : comedi : das1800 : prefer kmalloc_array over kmalloc with multiply
KEYS : potential uninitialized variable
ipx : call ipxitf_put () in ioctl error path
ipv6 : fix devconf after adding force_tllao option
perf intel - PT / BTS : Add missing initialization
scsi : bfa : release allocated memory in case of error
[ PATCH ] Fix oops in vfs_quotaon_mount ()
drm / virtio : Fix NULL vs IS_ERR checking in virtio_gpu_object_shmem_init
EDAC : Respect operational state in edac_pci . c
[ media ] pulse8 - cec : set correct Signal Free Time
sfc : don ' t BUG_ON efx -> max_channels == 0 in probe
tools : ffs - aio - example : add missing wMaxPacketSize for HS descs
ide : fix LOCKDEP warning
samples / bpf : deal with EBUSY return code from sendmsg in xdpsock sample
ipmi : fasync BKL pushdown
drm : fix uninitialized acquire_ctx fields ( v2 )
rpmsg : glink : The mbox client knows_txdone
sfc : initialize dynamic sysfs attributes for lockdep
vhost : actually track log eventfd file
s390 / qeth : use QDIO_ * _QFMT defines
perf sched : Look up thread using tid instead of pid
s390 / cpumf : add missing lpp magic initialization
x86 / mm : Fix use - after - free of ldt_struct
USB : option : make interface blacklist work again
cfg80211 : Clear " beacon_found " on regulatory restore
iwlwifi : mvm : disable scheduled scan
netrom : Fix use - after - free caused by accept on already connected socket
rapidio : switch to get_user_pages_fast ()
staging : fbtft : make const array gamma_par_mask static
iwlwifi : reduce max remain - on - channel duration
iommu / amd : Fix BUG when faulting a PROT_NONE VMA
jiffies : add utility function to calculate delta in ms
rtc : ds1374 : Set . of_match_table to OF device ID table
nvmet - rdma : occasionally flush ongoing controller teardown
USB : serial : console : fix use - after - free after failed setup
ath9k : Configure AR_PHY_SWITCH_COM with apropriate antenna control
ipv6 : ipv6_find_hdr restore prev functionality
drivers : thermal : parent virtual hwmon with thermal zone
xfrm_user : fix info leak in copy_to_user_auth ()
drm / radeon : do not hard reset GPU while freezing on r600 / r700 family
mmc : sdhci - spear : Handle return value of platform_get_irq
mlxsw : spectrum : Fix ageing time value
mac80211 : fix start_seq_num in Rx reorder offload
stmmac : add missing of_node_put
iio : iio_buffer_register : Skip channels with negative scan index
ieee1394 : SPIN_LOCK_UNLOCKED cleanup
e1000 : check on netif_running () before calling e1000_up ()
locks : fix list insertion when lock is split in two
keys : fix race with concurrent install_user_keyrings ()
cgroup : fix a leak when percpu_ref_init () fails
xen : make xen_flush_tlb_all () static
Btrfs : fix em leak in find_first_block_group
isdn : off by one in connect_res ()
TOMOYO : Fix incomplete read after seek .
USB : wusb : check CHID is all zeros before stopping the host
KVM : Fix pit memory leak if unable to allocate irq source id
ARM : davinci : neuros - osd2 : specify the chipselect in davinci_nand_pdata
USB : devio : fix issue with log flooding
fuse : don ' t wake up reserved req in fuse_conn_kill ()
[ PATCH ] USB : Resource leak fix for whiteheat driver
[ MIPS ] Sibyte : Fix name of clocksource .
Drivers : hv : fcopy : restore correct transfer length
cpufreq : Avoid calling cpufreq driver ' s target () routine if target_freq == policy -> cur
pppoe : Lacks DST MAC address check
perf thread_map : Correctly size buffer used with dirent -> dt_name
mac80211 : minstrel : avoid port control frames for sampling
[ SCSI ] libfc : fix payload size passed to fc_frame_alloc () in fc_lport_els_request
ext4 : remove unnecessary ext4_get_group_info in ext4_mb_load_buddy
mac80211 : Apply ht_opmode changes in ieee80211_change_bss
ARM : imx : call imx6sx_cpuidle_init () conditionally for 6sll
sctp : fix possible seqlock seadlock in sctp_packet_transmit ()
OMAPDSS : fix registering the vsync isr in apply
video : vt8500lcdfb : Remove kfree call since devm_kzalloc () is used
KVM : nVMX : No need for handle_vmx_insn function any more
ipmi : fix platform return check
ACPI : Add DMI entry for Sony VGN - FW41E_H
firewire : cdev : fix another memory leak in an error path
ISCSI : fix minor memory leak
usb : musb : blackfin : call gpio_free () on error path in musb_platform_init ()
au1xxx - ide : use ide_find_port ()
tracing : Uninitialized variable in create_tracing_map_fields ()
dm cache : fix leaking of deferred bio prison cells
usb : phy : r - car gen2 : use usb_add_phy_dev
RDS : null pointer dereference in rds_atomic_free_op
ARM : 8305 / 1 : DMA : Fix kzalloc flags in __iommu_alloc_buffer ()
ath9k : check for NULL rate when using ieee80211_get_rts_cts_rate ()
bnx2x : Add BCM84823 to the supported PHYs
kconfig : Avoid buffer underrun in choice input
watchdog : bcm7038 : Check the return value from clk_prepare_enable ()
perf probe : Load map before glob matching
m68k : macide doesnt check for Mac
dm crypt : use memzero_explicit for on - stack buffer
x86 , mce : reinitialize per cpu features on resume
vfs : ioctl : prevent double - fetch in dedupe ioctl
x86 / boot : Fix minor fd leakage in tools / relocs . c
USB : PD : define specific requests
memcg : hierarchy avoid unnecessary reclaim
greybus : svc : disable connection after all its users are gone
staging : lustre : libcfs : fix aligment issue
mm : enforce min addr even if capable () in expand_downwards ()
drm / radeon : add backlight quirk for hybrid mac
mt76 : fix variable gain adjustment range
iio : imu : adis16400 : fix memory leak
net_sched : cls_route : remove the right filter from hashtable
ALSA : asihpi : incorrect range check
gpio : Propagate all errors in devm_get_gpiod_from_child ()
[ SCSI ] bnx2i : Added reconnect fix connecting against Lefthand targets
kvm : nVMX : Don ' t allow L2 to access the hardware CR8
ASoC : da8xx / omap - l1xx : match codec_name with i2c ids
pinctrl : sh - pfc : add missing of_node_put
blk - mq : Wake tasks entering queue on dying
ARM : at91 / gpio : fix display of number of irq setuped
crypto : drbg - fix semicolon . cocci warnings
iommu / ipmmu - vmsa : IPMMU device is 40 - bit bus master
ALSA : bebob : Fix a missing to unlock mutex in error handling case
drm / amd / display : Check msg -> size before starting aux transfer
sfc : check for allocation failure
[ media ] V4L2 : mt9t112 : fixup JPEG initialization workaround
bnx2x : Calling pci_set_drvdata earlier
block : initialise bd_super in bdget ()
clk : imx7d : Correct ahb clk parent select
ipv6 : Add GSO support on forwarding path
rds : Fix NULL pointer dereference in __rds_rdma_map
tcp : initialize rcv_mss to TCP_MIN_MSS instead of 0
powerpc : Check for valid hugepage size in hugetlb_get_unmapped_area
NFSv4 : Make sure unlock is really an unlock when cancelling a lock
powerpc / bpf : Use memset32 () to pre - fill traps in BPF page ( s )
vhost : fix queue number check when setting inflight FD
ocsp : corrected the comparison of the serial size in OCSP response
libnsgif : Fix null pointer deref on frameless GIF input
encode : fix empty FIELD_2DD_VECTOR
secvar : fix endian conversion
stream : fix false negative on bad RST
[ core ] fix parse_via () parsing
[ commissioner ] add network name length check in GeneratePskc () (# 4404 )
Decoder : Set u1_long_term_reference_flag to 0 for error concealment
IGraphicBufferProducer : fix QUEUE_BUFFER info leak
LibCrypto : Don ' t copy the prime test candidates
Fix a shape inference issue leading to nullptr deref .
Handle one more division by 0 in TFLite .
Prevent dereferencing of null pointers in TFLite ' s ` add . cc `.
Fix the CHECK failure in tf . raw_ops . QuantizeAndDequantizeV2 .
[ tfg ][ functiondef_import ] Emit error on empty function attributes
bugfix (# 245 ) - long incoming message crashes server
fix possible format string exploit if packet dumping enabled
android : Stop child process in GetTerminationInfo
Add a check to prevent len from going negative in xmlParseAttValueComplex .
[ DevTools ] Use no - referrer for DevTools links
ash : Fix right - alignment of power - status text .
