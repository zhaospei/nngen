fish : Fix memory leak in inspect /- i code ( detected by valgrind ).
write_sensu plugin : malloc + memset -> calloc
cpu plugin : Collectd CPU ` steal time ' as reported by new Linux kernels .
* kdb . h ( krb5_def_promote_db ): Declare
Always stat profile files after reopening them
fall back on realm name as a default domain name
fixing small memory leak .
fix write () on a file desriptor of - 1 , bb # 5784
crmd : Provide useful feedback when the metadata is unreadable
wallet : Add error handling . Check return value of ParseUInt32 (...) in ParseHDKeypath (...).
RPC : strictly require HTTP URI "/"
util : handle NULL pointers passed to git_strarray_free ()
refs : fix unused - but - set warning
Added an extra check to MLU bounds
utils / transicc / transicc . c : Fix incorrect logical operator
daemon : unbreak NO_IPV6 build regression
fetch - pack : Finish negotation if remote replies " ACK % s ready "
git_connect_git (): forbid newlines in host and path
grep : fix colouring of matches with zero length
send - pack : avoid deadlock on git :// push with failed pack - objects
refs : prevent accidental NULL dereference in write_pseudoref
get_sha1_oneline : check return value of parse_object
[ PATCH ] Fix count - delta overcounting
http - push : do not SEGV after fetching a bad pack idx file
lib - master : Minor memory leak fix on deinit .
squat_trie_lookup (): Make sure we clear result arrays first .
lib - imap - client : Don ' t start another DNS lookup if there ' s already one ongoing .
POP3 : A bit more code for handling nicely unsupported auth mechanisms .
pgsql : Avoid epoll_ctl () errors when closing connection .
imapc : Fix crash in mailbox_exists () when LAYOUT isn ' t imapc .
dict - file : Updated dotlock settings .
lib - index : Compiler warning fix
lib - program - client : Fix memory access problem
lib - imap - storage : Fixed leaking istream on error conditions .
DEBUG : buffer_verify_pool () crashed with empty buffer .
memset () the returned structure , just in case .
lmtp : Fixed lmtp_user_concurrency_limit = 0 to actually mean unlimited
Importing ratbox3 r25213 changeset
fix compiler warning : comparison between signed and unsigned
ConnectionExists : make sure conn -> data is set when " taking " a connection
fix compiler warning
AddFormData : prevent only directories from being posted
free the allocated mailbox name at disconnect
conncache : fixed memory leak on OOM ( torture tests )
avoid proxy ( and other ) auth if no user + password is given
Prevent an assertion with invalid Unicode sequences
Added header to momctl unit test to resolve do_mom
Fixed problem with torque HA where trqauthd was
app_python3 : skip execution of child_init () for rank PROC_INIT
Initialize frame rate extension fields in mp_header_process_sequence_header
vo_opengl : free to - be - added hook on hook array overflow
vo_opengl : fix extension name
ad_spdif : Fix crash when spdif muxer is not available
vo : mark frame lost after seek reset
fix null pointer check in add_subtitles ,
fix broken term_osd output in update_osd_msg ()
vo_direct3d : fix bug in screenshot code with some pixel formats
reset channels too
DevAPI : for_each_ { managed , logical } _proc helper macros
Bugfix : hashbusterusb : Correct return value of hashbusterusb_vrm_unlock
typo in comment
Ran acpisrc source cleanup
PPC : e500 : correct params -> ram_size with ram_size
scsi - disk : fix changeable values for MODE_PAGE_R_W_ERROR
cpu : Add missing ' static ' attribute to qemu_global_mutex
posix - aio - compat : Fix async_conmtext for ioctl
gtk : set free_scale when setting zoom_fit
qcow2 : Fix refcount table size calculation
block / cloop : Use g_free instead of free
usb : sanity check setup_index + setup_len in post_load
qcow2 : Free preallocated zero clusters
slirp : Fix packet expiration
qemu : msix nit : clear msix_entries_nr on error
add missing ' break '
blkid : add DEVNAME = to export output format
agetty : fix memory leak [ clang - analyzer ]
libblkid : fix blkid_partlist_devno_to_partition () for extended partitions
swapon : missing separator in swapon - s command
added ignoreSet to rpmRunTransaction ()
http_jmap . c : gracefully handle NULL mailboxes on close
jmap : fix typo in calendarEventsSet response name
mailbox : add mkdir back to copyfile , and return IOERROR
mailbox : set header size on create so appends work
dav : don ' t double - close davdb on mailbox delete
carddav : don ' t crash on x - fm - otheraccount - member with no userid
Call clusterUpdateState () after CLUSTER SETSLOT too .
adding assert to protect delayed_pic
rl2 : Avoid a division by zero
rtmppkt : Make pkt -> data reallocable
xa : fix end - of - file handling
libxavs : add an AVClass at the beginning of XavsContext .
dv : Replace a magic number by sizeof ()
handle http error codes
decode h264 end - of - slice flag
don ' t close a nonexistent connection ( null dereference )
Releases don ' t get replied to
mqtt : Copy UAT match_criteria value
Removed an unneeded cast .
Add a description for errno EINVAL ( Invalid Argument ) to
epan_init (): make ' status ' volatile ( to avoid might - be - clobbered warnings ).
Try to make Mac OS X Buildbot Happy
don ' t read empty files
forgot to decrement result_allocated :(
fix for bug # 32282 ( segfault in mysqli_fetch_array on 64 - bit )
lib : Make flatpak_installation_drop_caches return TRUE on success
[ common / flatpak - run ] add / dev / mali0 to -- device = dri
switch - root : do not use close old_root_fd after rm_rf_children ()
systemctl : also highlight a load state of " not - found " as red
udev : properly detect reference to unexisting part of PROGRAM ' s result
udev : kmod - fix typo
ask - password : use kill ( PID , 0 ) before querying a password
sd - rtnl : rtnl_call - don ' t dispatch wqueue after timeout has passed
selinux : reopen log when loading the SELinux policy fails
dhcp - server : fix integer underflow
check that ctx is not a null ptr before deref cid # 140
select heimdal_kuser as text domain
Fixed memory leak .
Fixed # 6013 : Check new length is > 0
libfreerdp - core : server fix sending zero length update pdu .
Fixed missing stream length check in drive_file_query_directory
Added assistance file name NULL check .
codec : Another realloc fix
Fixed Out - of - bound read in glyph_cache_put
avformat / movenc : Check input sample count
indeo5 : check against scaleable frames in non scaleable streams .
avcodec / options : use av_opt_copy () in avcodec_copy_context () to copy priv_data
electronicarts : prevent overflow during block alignment calculation
add support for PCM codec
dshow : avoid integer overflow when using curbufsize
avcodec / atrac3 : Check init_get_bits8 () for failure
avformat / rmenc : Check for av_malloc () failure
rtmppkt : Make pkt -> data reallocable
tscc2 : fix out of array access
avformat / cinedec : Fix DoS due to lack of eof check
avcodec / hevc_refs : Check nb_refs in add_candidate_ref ()
avformat / hls : fix memory leak with non - http segments
Check the allocated pointer instead of the given pointer as intended
rv10 : Forward error from rv10_decode_packet
avcodec / avpacket : Use av_free_packet () in error cleanups
postproc : fix null pointer dereference with invalid option strings
vformat / tee : fix uninitialized use of ret
gxfenc : fix null ptr dereference
svq1enc : fix signed multiplication overflow
vorbis_parser : Move vp check to avoid a null pointer dereference
avcodec / h264_mvpred : Fix multiple runtime error : left shift of negative value
ffserver : avoid leaking pathname at exit
avcodec / mpegaudiodec_template : Check for negative e
avcodec / ffv1dec_template : Fix signed integer overflow
riff : fix invalid av_freep () calls on EOF in ff_read_riff_info
avoid uninitialised variable warning and clarify code
electronicarts : check bytes per sample for validity
avcodec / diracdec : Fixes integer overflow
avcodec / rv34 : fix crash while seeking on very damaged file
mpegvideo_enc : reduce QMAT_SHIFT to avoid overflow in dnxhd
asrc_aevalsrc : Fix use of uninitialized pointer inside av_strtok ()
avcodec / truemotion2 : Fix integer overflows in tm2_high_chroma ()
avcodec / get_bits : Fix get_sbits_long ( 0 )
avcodec / pthread_slice : Use av_freep () to avoid leaving stale pointers in memory
librtmp : append the correct field to the string
lavfi : af_amerge options array was not ended with NULL
avcodec / lagarith : disable lag_decode_zero_run_line () and ask for a sample
ffplay : Fix usage of private lavfi API
rtpproto : Check the size before reading buf [ 1 ]
lavc / alac : Avoid allocating huge memory blocks for malicious alac input .
lavfi / unsharp : fix opencl crash on 64bit linux
h264 : Fix parameters to ff_er_add_slice () call
avcodec / mlpdec : Fail if the input is too small
lavf / mpeg : suppress warning : lpcm_header_len may be used uninitialized in this function
avformat / mxg : Use memmove ()
indeo5 : fix division by 0 in ff_ivi_init_tiles ()
libavutil / opencl : fixed uninitialized var warning
avcodec / libxvid : fix potential integer overflow
vp56 : Check for missing reference frame data
avformat / pva : Check for EOF before retrying in read_part_of_packet ()
adpcm : Fix crash
avformat / cache : Avoid int - overflow in cache compare function
Fix a crash in the H . 263 RTP packetizer
Fix out of bound reads in the QDM2 decoder .
swscale : don ' t use planar output functions to write to NV12 / 21 .
avcodec / mips / aaccoder_mips : Disable ff_aac_coder_init_mips () to prevent build failure
av_find_stream_info (): set subtitle codec time_base
avformat / gxfenc : Clear nb_fields on array deallocation
lavfi : avfilter_merge_formats : handle case where inputs are same
vc2enc_dwt : use 32 bit coefficients by default
avcodec / x86 / cavsdsp : Put MMX code under mmx check
indeo5 : fix null pointer crash with ref_mb
avcodec / mlpdec : Fix runtime error : left shift of negative value - 1
lavf : Dont compare absolute to relative timestamps in duration gcd
avcodec / mpegaudiodec_template : Make l3_unscale () work with e = 0
rtmpproto : Send chunk size on the network channel
Avoid a null pointer dereference on oom when decoding smacker .
examples / muxing : check on frame
avcodec / movtextdec : Make tx3g_ptr unsigned
avcodec / hevc_cabac : decrease CABAC_MAX_BIN
ffplay : avoid frame data leak on early frame drop
Fix a read past end of buffer crash in the mp3 probe
mov : Fix near infinite loop in stsd parsing .
swscale : align vertical filtersize by 2 on x86 .
lzo : fix overflow checking in copy_backptr ()
avformat / mov : use 64bit for size in mov_skip_multiple_stsd ()
vda : better frame allocation
avcodec / proresdec2 : SKIP_BITS () does not work with len = 32
http : Check for negative chunk sizes
matroskadec : Fix buffer overread in matroska_ebmlnum_uint
libx264 : do not set pic quality if no frame is output
bink : check vst -> index_entries before using it
parser : fix large overreads
aacsbr : always initialize max_qmf_subbands
lzw : fix potential integer overflow .
libopenjpegenc : add NULL check for img before accessing it
avcodec / mjpegdec : Fix integer overflow in DC dequantization
avcodec / vorbis_parser : Move vp check
aacdec : drop channel reseting code .
avcodec / mlz : clear dict on allocation to ensure there are no uninitialized values
avcodec / escape124 : Check depth against num_superblocks
lavf / movenc : check ff_mov_init_hinting () return
avcodec / smc : fix off by 1 error
jpeg2000 : check len before parsing header
avcodec / lagarith : Check scale_factor
adxenc : check return value of avcodec_alloc_frame ()
avcodec / ac3_parser : Check init_get_bits8 () for failure
avcodec / mjpegdec : Check for pixfmtid 0x42111100 || 0x24111100 with more than 8 bits
fix memory leak in oggenc
aacsbr : Silence warning : max_qmf_subbands may be used uninitialized in this function
Fix out of bound writes in fix_bitshift () of the shorten decoder .
avcodec / hnm4video : check that packetsize is at least 8
lavfi / amerge : drop duplicated " Buffer queue overflow " message
cook : silence some signed overflow warnings .
avcodec / wavpack : Fix integer overflow
avcodec / mlz : Remove ' l ' postfixes from numbers
lavu : prevent overflow in av_clip_intp2_c
avcodec / dcadsp : Fix runtime error : signed integer overflow
avcodec / wavpack : Fix runtime error : left shift of negative value - 5
Avoid a null pointer dereference on oom in the aac encoder .
avformat / thp : Check av_get_packet () for failure not only for partial output
v410enc : fix output buffer size check
avcodec / mpeg4videoenc : Use 64 bit for times in mpeg4_encode_gop_header ()
avcodec / truemotion2 : Fix passing null pointer to memset ()
h264 : prevent rbsp_buffer values from becoming duplicated
avcodec / h264_refs : Fix long_idx check
avcodec / utils : fix memleak on avcodec_open2 () failure
mxfdec : Include FF_INPUT_BUFFER_PADDING_SIZE when allocating extradata .
dashdec : Only free url string if being reused
cdxl demux : do not create packets with uninitialized data at EOF .
Provide a fallback version of the libm function trunc
avformat / mxfdec : Fix Sign error in mxf_read_primer_pack ()
avformat / rmdec : Check for overflow in ff_rm_read_mdpr_codecdata ()
avformat / riffenc : Use size_t for strlen in ff_riff_write_info_tag ()
avformat / mxfenc : Set color siting to 0 for D10 - MXF
avcodec / ffv1dec : Free tables on init failure
avformat / avio : Fix null pointer dereference in case of memleak
avcodec / ffv1dec : Explicitly check read_quant_table () return value
aacdec : fix " may be used uninitialized " warning
lavf : fix the comparison in an overflow check
doc / examples / decoder_targeted : Limit max pixels for fuzzing
avcodec / pngenc : Initialize fctl_chunk to 0
wmadec : avoid infinit loop .
avcodec / hnm4video : change width / height to int
avformat / pjsdec : dont increase pointer when its already at the end in read_ts ()
ffmpeg : dont call exit_program () from a signal hander
aacenc_ltp : fix assertion
pmpdec : Check for zero audio packets .
avcodec / msmpeg4dec : Check for input end in msmpeg4v34_decode_mb ()
ff_gen_search : make step 64it to prevent hypothetical integer overflow
swresample / swresample : Use av_mallocz_array ()
tm2 : Check remaining size before init_get_bits ()
lavf / rtsp : Fix a crash with the RTSP muxer .
mpegvideo : initialize videodsp with correct pixel depth
avcodec / mpeg4videoenc : Use 64 bit for times in mpeg4_encode_gop_header ()
libavcodec / wmalosslessdec : add missing emms
ffmpeg : fix uninitialized variable warning
mss1 : improve check if decoded pivot is invalid
avcodec / h264_slice : Fix signed integer overflow
avcodec / dvdsubdec : Fix off - by - one error
lavfi / hue : fix crash when resetting the saturation expression
examples / avio_reading : fix null dereference on error
avcodec / ulti : Check number of blocks at init
avcodec / pngdec : Clean up on av_frame_ref () failure
libopusenc : check return value
dv : Initialize encoder tables during encoder init .
avcodec / mlpdec : Fix runtime error : left shift of negative value - 22
mxfdec : Fix files > 2 GiB
avformat / oggdec : check for av_packet_new_side_data () failure
mov : Fix negative size calculation in mov_read_default ().
avcodec / indeo4 : Check remaining data in Pic hdr extension parsing code
lavf / hlsenc : fix basename size computation off - by - one bug
avcodec / jpeg2000 : Check sum of sizes of band -> prec before allocating
Fix a mem leak in av_find_stream_info ().
avformat / movenc : Fix use of uninitialized variable ( ret )
avformat / movenc : avoid using uninitialized display_matrix_size
vp56 : alpha_offset is uninitialized on purpose
swfdec : check lens validity
avformat / oggdec : Fix integer overflow with invalid pts
avprobe : Zero the allocated avio buffer memory
vc1dec : prevent v_edge_pos from becoming negative .
avformat / mov : fix avg_frame_rate calculation
lavf / vf_framerate : Fix frame leak when increasing framerate .
ffmpeg : Fix bsf corrupting merged side data
libx264 : Define X264_API_IMPORTS on MSVC / ICL
avcodec / bmp : fix integer overflow in checking header sizes
vf_hqdn3d : make config_props work properly when called multiple times .
avcodec / takdec : Fix integer overflow in decode_lpc ()
avformat / vqf : check number of channels before use .
check for sane values in ogg_get_length (), fix lol - ffplay . ogm fuzz test
mp3enc : fix CODEC_ID / CodecID that leaked in from older patch
avcodec / dvdsub_parser : never return 0 when the input isnt 0
yop : check return value of avformat_new_stream ()
codec_desc : mark fraps as an intra - only codec .
Arrays where one element too small , fixes CID114 .
jvdec : Return EOF on end of file
swscale - example : Don ' t return random value from main ().
mlpdec : dont leave a invalid huff_lsb in the context .
pan : fix uninitialized channel_id variable .
rv34 : Avoid NULL dereference on corrupted bitstream
rtpdec : Limit writing to the buffer size
ffplay : add existence check on ic -> pb in decode_thread ()
avcodec / wnv1 : More strict buffer size check
avformat / dashdec : Fix memleak when resolve_content_path
avcodec / vda_h264 : fix null pointer dereference
mem : Make av_strdup allocate using av_realloc
avformat / adxdec : dont return random data on EOF
lavf / matroskaenc : fix uninitialized read
avcodec / vdpau_hevc : Fix potential out - of - bounds write
matroska : Clear prev_pkt between seeks .
avformat / aviobuf : Fix signed integer overflow in avio_seek ()
avcodec / cfhd : Fix undefined shift
lavf / img2dec : fix out - of - range check in find_image_range ()
avcodec / h264 : Only reinit quant tables if a new PPS is allowed
Check for out of bound writes when building tree in bink decoder .
avformat / dfa : check first frame offset in probe
avoid deprecation warnings for AVFrac from avformat . h
avcodec / mpegvideo_enc : Check for integer overflow in ff_mpv_reallocate_putbitbuffer ()
lavc / jpeg2000dec : Fix jp2 inner atom size used for overread checks .
lavc / libschroedinger : add missing failure checks in ff_create_schro_frame ()
avcodec / rv40 : Test remaining space in loop of get_dimension ()
eacmv : check for out of bound reads
libavformat : Check for malloc failures in avformat_new_stream
libx265 : Fix ' braces around scalar initializer ' warning
avformat / utils : fix memory leak in avformat_free_context
vc1_parser : fix NULL pointer dereference
avformat / hls : do not limit manifest lines to 1024 chars
supporting rare overflow mess even with error_resilience >= 0
gifdec : fix invalid write in gif_copy_img_rect
g723_1 : fix overflow in square_root ()
zerocodec : check if there is previous frame
avcodec / webp : Fixes null pointer dereference
oggparseogm : check timing variables
mm : Skip unexpected audio packets
mjpegdec : rgb mode is specific for ljpeg , disable it for others .
avcodec / pafvideo : Fix assertion failure
avcodec / dnxhddec : reset cur_field for non interlaced frames
vc2enc : fix segfault
avcodec / hevcdsp_template . c : Fix undefined shift in FUNC ( dequant )
avcodec / hevc_parser : check for av_mallocz () failure
smvjpegdec : make sure cur_frame is not negative
avcodec / dxva2_h264 : Fix " may be used uninitialized " warnings
vsrc_testsrc : fix off - by - one logic when detecting the EOF time in request_frame ()
avformat / matroskadec : fix resource leak
checkasm : Give macro a body to avoid potential unexpected syntax issues
libavcodec : Don ' t do av_free ( av_malloc ( 0 )) for bitstream filters
avcodec / tiff : reset sampling [] if its invalid
cfhd : fix off - by - one error in level check
avcodec / dxv : Check for end of input in dxv_decompress_dxt5 ()
quickdraw : Make the palette opaque
AMV : disable DR1 and don ' t override EMU_EDGE
h264 : cap max has_b_frames at MAX_DELAYED_PIC_COUNT - 1 .
ac3 : validate end in ff_ac3_bit_alloc_calc_mask
http : Don ' t use the normal http proxy mechanism for https
fixed double free
avcodec / hevc : check slice_header_extension data length
mpegvideo : synchronize AVFrame pointers in ERContext fully
flacdec : fix buffer size checking in get_metadata_size ()
bink : Check for out of bound writes when building tree
Return early from NVS command submission if no response is required
qemu - sockets : do not test path with access () before unlinking
tests : fix virtio - scsi - test leak
acl : Fix acl_remove not to mess up the ACL
target - i386 : Fix jmp im on x86_64 when executing 32 - bit code
linux - user : Check dump_write () return in elf_core_dump ()
pulse - audio : fix bug on updating rpos
ehci : clear suspend bit on detach
virtio : avoid buffer overrun on incoming migration
esp - pci : fixup deadlock with linux
blockdev : use drained_begin / end for qmp_block_resize
fix off - by - one error in pci_piix3_xen_ide_unplug
macio : fix overflow in lba to offset conversion for ATAPI devices
block / qapi : Plug memory leak in dump_qobject () case QTYPE_QERROR
target - ppc : free a tcg temp variable
target - alpha : Suppress underflow from CVTTQ if DNZ
ahci : clear aiocb in ncq_cb
qcow2 : Use BB for resizing in qcow2_amend_options ()
target / ppc : Fix setting of cpu -> compat_pvr on incoming migration
target - i386 : key sfence availability on CPUID_SSE , not CPUID_SSE2
qga : don ' t fail if mount doesn ' t have slave devices
ahci : advertise HOST_CAP_64
vfio / common : cleanup in vfio_region_finalize
cuda . c : return error for unknown commands
ppc : fix monitor access to CR
esp : delay Transfer Information command if dma is not enabled
linux - user : return EINVAL from prctl ( PR_ * _SECCOMP )
pvpanic : use get_uint () for " ioport " property
slirp : fix segv when init failed
cpu : verify that block -> host is set
linux - user : In fork_end (), remove correct CPUs from CPU list
qemu - img : fix invalid JSON
usb : sanity check setup_index + setup_len in post_load
qdev : Fix crash on - device '?= x '
scsi : megasas : initialise local configuration data buffer
block / drive - mirror : Reuse backing HD for sync = none
qemu - ga : Plug fd leak on ga_channel_listen_accept () error path
qdev : Fix use after free in qdev_init_nofail error path
qemu - char : Fix crash when switching consoles
Bugfix : PowerPC 64 slbia never invalidates the first segment entry .
vvfat : fix qemu - img map and qemu - img convert
hw / pc_sysfw : Fix memory leak
block / nbd : fix segmentation fault when . desc is not null - terminated
block : Reset buffer alignment on detach
x86 : translate . c : remove dead assignment
pcie_aer : clear cmask for Advanced Error Interrupt Message Number
tests : Fix two memory leaks
misc : zynq - xadc : Fix off - by - one
ppc : Let kvmppc_reset_htab () return 0 for ! CONFIG_KVM
target - arm : Signal Underflow when denormal flushed to zero on output
s390x / kvm : fix small race reboot vs . cmma
linux - user : pause () should not pause if signal pending
vhost : fix double free on device stop
qga : fix ' driver ' leak in guest - get - fsinfo
virtio : decrement vq -> inuse in virtqueue_discard ()
block : Limit size to INT_MAX in bdrv_check_byte_request ()
block : Limit request size ( CVE - 2014 - 0143 )
block / vpc : fix uninitialised variable compiler warning
target - i386 : svm : Fix MSRPM check
blkverify : Fix BDS leak in . bdrv_open error path
target - arm : fix incorrect temporary variable freeing
net : make iov_to_buf take right size argument in nc_sendv_compat ()
drive mirror : fix memory leak
ppc : fix crash in ppc system single step support
block : Handle flush error in bdrv_pwrite_sync ()
spapr_drc : Make device " spapr - dr - connector " unavailable with - device
pci / shpc : fix signed integer overflow
linux - user / mmap . c : Avoid choosing NULL as start address
chardev : use backend chr context when watch for fe
rdma : fix memory leak
spapr : fix possible memory leak
usb : ehci : fix memory leak in ehci_process_itd
ui / console : fix OVERFLOW_BEFORE_WIDEN
replay : bh scheduling fix
aio - posix : honor is_external in AioContext polling
cpu - exec : Also reload CPUClass * cc after longjmp return in cpu_exec ()
PPC : Fix TLB invalidation bug within the PPC interrupt handler .
hw / intc / exynos4210_combiner : Don ' t overrun output_irq array in init
io : remove mistaken call to object_ref on QTask
hw / virtio - 9p - local . c : Remove unnecessary null char in symlink file
tests : check - qom - proplist : fix leak
vhost : fix a memory leak
rbd : Fix leaks in rbd_start_aio () error path
sd : Avoid access to NULL BlockDriverState
tcg / ppc : Do not overwrite lower address word on Darwin and AIX
ARM : fix segfault
vvfat : fix a file descriptor leak
vmdk : Fix overflow if l1_size is 0x20000000
linux - user : Fix locking order in fork_start ()
fix bug in block - qcow2 . c : alloc_cluster_offset () ( Shahar Frank )
gdbstub : fix for commit 87f25c12bfeaaa0c41fb857713bbc7e8a9b757dc
x86 / cpuid : Plug memory leak in cpudef_setfield ()
ppc : fix double - free in cpu_post_load ()
virtio : increase the queue limit to 1024
watchdog / wdt_diag288 : Mark diag288 watchdog as non - hotpluggable
ossaudio : fix memory leak
vhost - scsi : fix k -> set_guest_notifiers () NULL dereference
exec . c : fix dirty bitmap reallocation
chardev / baum : fix baum that releases brlapi twice
slirp : fix use - after - free
runstate / migrate : Two more transitions
target - mips : Fix type cast for w64 ( uintptr_t )
vnc : Fix crash on ' info vnc ' after ' change vnc none '
qxl : call dpy_gfx_resize when entering vga mode
scsi : fix warning
block / raw - posix : Fix disk corruption in try_fiemap
hw / pci : fixed error flow in pci_qdev_init
spapr : fix memory leak in spapr_memory_pre_plug ()
sdl2 : fix crash in handle_windowevent () when restoring the screen size
qdev : Change Property :: offset field to ptrdiff_t type
qdev : fix thinko leading to guest crashes
usb : check USB configuration descriptor object
pty : Fix byte loss bug when connecting to pty
tcg : Reload local variables after return from longjmp
pcie : fix improper use of negative value
qemu - sockets : Fix buffer overflow in inet_parse ()
virtio : add missing mb () on enable notification
target - arm : fix for exponent comparison in recpe_f64
ccid : Fix buffer overrun in handling of VSC_ATR message
cpus . c : add additional error_report when ! TARGET_SUPPORT_MTTCG
target - arm : Fix TCG temporary leaks for scalar VMULL
qxl : clear guest_cursor on QXL_CURSOR_HIDE
target - i386 : Don ' t left shift negative constant
hw / i386 / acpi - build . c : Fix memory leak in acpi_build_tables_cleanup ()
tests : fix ipmi - kcs - test leak
fsdev : Fix overrun after readlink () fills buffer completely
irq : fix memory leak
scsi - generic : check the return value of bdrv_aio_ioctl in execute_command
hw / display / framebuffer . c : Avoid overflow for framebuffers > 4GB
spapr : drop reference on child object during core realization
spapr : ensure we have at least one XICS server
exec : Add missing rcu_read_unlock
APIC : Do not start zero - period timers ( Jan Kiszka )
pci : Fix memory leak
block : Don ' t use error_abort in blk_new_open
vga : Silence bogus gcc warning about uninitialized variables
chardev : clear O_NONBLOCK on SCM_RIGHTS file descriptors
hw / arm / omap_gpmc : Avoid buffer overrun filling prefetch FIFO
fsdev / virtfs - proxy - helper : Fix improper use of negative value
fix memory leak
Fix a memleak
Corrected a condition on the first op
Check if debug is dead in r_debug_kill function .
Oops for ' dk '
pid - 1 is not our child
Delete the right list item .
check for mules before nuke
dont allow empty indexes in wml variables
fix default room creation policy on server
made it so AI players have an empty description in multiplayer games
locations not on the map should instantly fail to location filter
AI : ensure multiple weapons are always checked for [ disable ] special
sd - dhcp - server : fix a leak
conf - parser : fix strv parsing
fuzz : fix coding style
mount : fix for complex automounts
udev : settle should return immediately when timeout is 0
base - filesystem : avoid all searching if the link already exists
exit - on - idle : only exit if actually idle
Patch to log security warnings when invalid identifier is used in message
[ windows ] init locale_dir only once
[ libview ] Do not jump to link dest when clicking with GDK_CONTROL_MASK
Bug 493778 – metadata plug - in crashes on some images
app : prevent warnings in gimp_levels_tool_config_notify ()
Bug 645120 - Disable color tools overlay dialogs
[ Indic ] Disable ' kern ' in Uniscribe bug compatibility mode
fix breakage on qnx
branches / zip : btr_cur_optimistic_insert (): Remove a redundant condition .
branches / zip : Deny non - superuser access to the INFORMATION_SCHEMA
Addresses # 1683 Assert shutdown is not called twice on the same minicron
sco compile fix : MAXPATHLEN
Fix a warning about extra parenthesis
Fixed a bug in vis . c
fix for decimal . c
Fix an off - by - one buffer size in dirserv . c that magically never hit our
Fix a double - free in rend_config_services ()
fix crash introduced in r9622
clean up a log message
Fix a potentially useless integer overflow check .
Added < utility > based on C ++ 11 for R - values and std :: move
Reverted change in config . h as it does not fix anything .
fix erasepage on linux
fixed minor memory leak
Always pass non - NULL status reference to job_waitjid , that function in
fix extra space
set version to 0 . 5 - dev
fix leak on group_layers
do not exit after the first read !
[ jit ] Fix the handling of byref locals in mono_allocate_stack_slots2 ().
Fix a JIT crash when - v - v - v - v is used .
make sure destructors are called , cleaning up socket
geoipbackend : Avoid expensive destroy during initialization
fixed # 1783 ( fuzz )
[ bouqueau ] fix win32 / mobile build
jcop : coverity scan warning RETURN_LOCAL
fix trivial leak
reader - pcsc . c : fixed pin checking when implicitly given
remove pointer to freed secret object
libopensc : initialize ' sc_ef_atr ' type variable
prevent out of bounds write
added a check for CKA_CERTIFICATE_TYPE to attr_extract ()
Check that result tupdesc has exactly 1 column in return_next scalar case .
adding Content - Length for HEAD request
[ XrdCl ] Prevent segv in case user has no name .
[ XrdCl ] Initialize pStateRedirec to avoid a segfault while recovering
groupserv : Actually free mygroups and groupacs .`
fixed sendfile core dump .
win : Fix setting icon after format of atom . rc has changed .
ospf6d : fix off - by - one on display of spf reasons
regexp_release : allow NULL regexp
* src / transform . c ( transform_save ): append newline to old text
allow std :: string in setContentType as before
prevent out - of - bounds ensure clause access ; fix # 2910
peephole optimization for LOADNIL before STRCAT ; ref # 3110
set the received message to zero , to make sure
pluto : undo bug introduced in 0395c26ac17af9b02e4f1b84d7279288fc985ced
Prevent the LDAP validator from accepting an empty password .
cherokee_buffer_crc32 () did not handle empty buffers correctly .
xkbcomp : fix stack overflow when evaluating boolean negation
x11 : add 0 < ctrls -> numGroups <= 4 assertion
src / utils : check if fileno () failed in map_file
Free the WSTR version of the filename when dead .
Added raptor_new_identifier_from_double pointer
Forgot to factor in softness
reset " chunked " flag when sending non - chunked reply
fix another memory leak
palm : getpeername may fail , found by coverity
recorder : Don ' t compute the buffer size twice
browser - plugin : Set that we need XEmbed
stream : cast memcap checks to uint64_t
ssh : fix out of bounds read in banner parsing
fix seg fault due to wrong sm list access in hscd
cuda : fix invalid use of sizeof
detect : free rule group init memory at the end of startup
rt : Fix size miscalculation in ivec_reserve
Allow compiling under Android
fix compiler warning
Fixed debug . c to work again unchanged .
[ MP ] Fix an incorrect memcpy count argument in CQuickSpriteSystem :: Add .
st . c : fix num_entries
Windows : terminate more aggressively when you close UnrealIRCd .
runtime : remove unnecessary ROUND in alg . c
Fix a possible crash in dsa_builtin_paramgen2 .
make sure we don ' t write to seed [- 1 ]
Avoid a memory leak in OCSP_parse_url ().
Remove an unused variable assignment
Gifsicle : Free Clp_Parser before exit to clean valgrind report .
libsepol / cil : Destroy classperm list when resetting map perms
imudp : add error msg when listener wasn ' t created
Check the result of EC_GROUP_new_curve_ * calls
Handle NO response without message properly
klondike - fix isc uninit warning
Bugfix : Missing pool_no parameter to applog for no - stratum - sessionid debug message
Fixing exit - after - terminate when used with bare mode .
Specialize the scrollbars for each platform
cli : remove superfluous problem_data_free () call
mbedtls : print warning if random personalisation fails
IEEE 802 . 11 : Add a bounds check
Fetch a 32 - bit big - endian quantity with EXTRACT_32BITS ().
tag server dirty if changing vars
send . c : if send_ra fails , return - 1
remove a debug message ; appears far too often in normal use
initialize unused pointer to prevent freeing without allocating
idevicebackup2 : Only post " sync finished " notification if we sent " sync start " earlier
Avoid a memory leak on non - PAM systems
API : Deprecate __index__ for ndim > 0
Fixing compile error from MSVC . Try 2 .
msg : MOSDSubOp has a private destructor
msgr : fix operator << for sockaddr_in , too
filejournal : initialize header before writing
mds : shut up about request latency
librbd / internal . cc : close resource leak
kclient : fix snap_rwsem link on non - existent cap release
mon : initialize vars
correct error of repeatedly adding io_size to off in do_bench_write
rgw : bi_list () initialize is_truncated if - ENOENT
filestore : sync object_map on _set_replay_guard ()
OSDMap : reset osd_primary_affinity shared_ptr when deepish_copy_from
librados : Allow rados_pool_list () to get NULL buffer if len == 0
ReplicatedPG : CEPH_OSD_OP_WATCH return - ENOENT if ! obs . exists
auth : delete Exception once it ' s caught ; fixing minor memory leak
Fixed a memory leak in the BMP decoder
libvncclient : free vncRec memory in rfbClientCleanup ()
ipf : release unhandled packets from the batch
hfs : fix keylen check in hfs_cat_traverse ()
branches / zip : Check for autoinc next value overflow explicitly when offset
Added missing DBUG_RETURN
Tracing test failure : testRedo - n CheckLCPStartsAfterNR T1
branches / zip : buf_page_init_for_read (): Increment the buf_fix_count
Fixed error in merge
lib / string . c : add ULL suffix to the constant definition
x86 : properly initialize temp insn buffer for paravirt patching
drm / i915 : Make disable_cursor_plane similar to commit_cursor_plane .
e1000 : only enable TSO6 via ethtool when using correct hardware
userns : don ' t leak root user
staging : clocking - wizard : check for a valid clk_name pointer
staging : comedi : pcmuio : fix the last > 80 char line warning
ASoC : OMAP : HDMI : Initialize IEC - 60958 channel status word
xen - gntalloc : integer overflow in gntalloc_ioctl_alloc ()
crypto : atmel - sha - add new version
igmp : Avoid zero delay when receiving odd mixture of IGMP queries
drm / vmwgfx : Fix kernel NULL pointer dereference on older hardware
[ PATCH ] fix Bug 4395 : modprobe bttv freezes the computer
staging : sm750fb : fix style for if clause
ceph : add missing locking to protect i_snap_realm_item during split
fix a race in ks8695_poll
ASoC : wm8994 : Move wm_hubs callback before we start ramping VMID
ath10k : add module parameter values to the pci info print
mm : vmalloc search restart fix
[ IPV6 ]: Fix ipv6 GSO payload length
iwlwifi : fix situation in which debug message is printed
[ PATCH ] lockdep : annotate scheduler runqueue locks
m68k : serial167 missing return value in cy_put_char ()
[ ALSA ] Add missing compat ioctls for ALSA control API
iio : ti_am335x : Remove redundant call to iio_sw_buffer_preenable ()
fs : fix data - loss on error
[ SCSI ] More USB deadlock fixes
staging : rtl8723au : Fix the indentation of two lines
spectra : Don ' t overwrite nand_debug_level . It ' s a module param
mfd : Use devm_ * APIs for mc13xxx : remove leftover kfree
tcp : remove unnecessary tcp_sk assignment .
ieee802154 : 6lowpan : Drop PACKET_OTHERHOST skbs in 6lowpan
mfd : add support for WM8351 revision B
iscsi class : fix get_host_stats error handling
ARM : 7803 / 1 : Fix deadlock scenario with smp_send_stop ()
[ SCSI ] fc_transport : Check portstates before invoking target scan
staging : ozwpan : Increase interrupt end point buffer size
sh : Zero - out coherent buffer in consistent_alloc ().
ext4 : use the correct error exit path in ext4_init_inode_table ()
ncpfs : successful rename () should invalidate caches for parents
ceph : fscache : Update object store limit after file writing
ath5k : free sc -> ah in ath5k_deinit_softc
perf record : Use sw counter only if hw pmu is not detected
perf / x86 / amd : Fix RDPMC index calculation for AMD family 15h
drm / amdkfd : unmap VMID <--> PASID when relesing VMID ( non - HWS )
mm : memblock : fix wrong memmove size in memblock_merge_regions ()
cfg80211 : enter psm when working as p2p_cli
[ media ] r820t : better report signal strength
ARM : shmobile : r8a7778 : add MMCIF clock support for DT
Btrfs : fix false positive in check - integrity on unmount
staging : zcache : fix possible sleep under lock
[ media ] V4L : mt9t112 : use after free in mt9t112_probe ()
twl - regulator : Fix reg_disable functionality for 4030 and 6030
ALSA : usb - audio : Fix format string mismatch in mixer . c
ipv6 : fix headroom calculation in udp6_ufo_fragment
leds : 88pm860x : add missing of_node_put
virtio_net : zero is an invald queue_pairs number
cxgb4 : allow large buffer size to have page size
dm raid1 : switch rh_in_sync to blocking in do_reads
sky2 : use roundup () macro
igbvf : clear buffer_info -> dma after dma_unmap_single ()
perf stat : Using init_stats instead of memset
ixgbe : Return PCI_ERS_RESULT_DISCONNECT when bus is disabled
mtd : cmdlinepart : allow small partitions
spi : img - spfi : Enable controller before starting TX DMA
[ NETFILTER ]: Remove okfn usage in ip_vs_core . c
perf tools : Disallow PMU events intel_pt and intel_bts until there is support
include / asm - x86 / setup . h : checkpatch cleanups - formatting only
HID : wacom : Do not repeatedly attempt to set device mode on error
video : udlfb : Use NULL instead of 0
powerpc / vphn : NUMA node code expects big - endian
macb : Fix speed setting
ASoC : max9867 : silence and array overflow warning
ASoC : Staticise pxa2xx_pcm_ops
x86 / PCI : move final fixups from __init to __devinit
tcp : skb_mstamp_after helper
firewire : fix failure path in ohci_enable_phys_dma
sched : add sanity check in partition_sched_domains ()
[ PATCH ] sysfs : fix a kobject leak in sysfs_add_link on the error path
[ PATCH ] manage - jbd - its - own - slab fix
x86 / nmi : export local_touch_nmi () symbol for modules
bonding : bond_xmit_roundrobin () fix
amd - iommu : set evt_buf_size correctly
[ media ] marvell - cam : Don ' t signal multiple frame completions in scatter / gather mode
perf tools : Fix new_term () missing free on error path
[ media ] pvrusb2 : Fix a few missing default control values , for cropping
drm / mgag200 : check alloc_apertures () success in mga_vram_init ()
[ X25 ]: Restore skb -> dev setting in x25_type_trans ().
drm / i915 : Reject opening of pipe crc files for invalid pipes
rcu : Export rcu_scheduler_active
mfd : Copy the device pointer to the twl4030 - madc structure
[ SCSI ] virtio_scsi : fix memory leak on full queue condition .
Input : ALPS - fix command mode check
leak in do_ubd_request
mac80211 : use RCU read locks for sta_info_get
drm / radeon : don ' t allow device to be opened if powered down
mnt : Carefully set CL_UNPRIVILEGED in clone_mnt
USB : chaoskey read offset bug
um : Ensure that a stub page cannot get unmapped
drivers / staging / lustre : Fix set - but - unused whinge .
tty : moxa : fix bit test in moxa_start ()
libata - sff : Don ' t call bmdma_stop on non DMA capable controllers
tridentfb : fix hang on Blade3D with CONFIG_CC_OPTIMIZE_FOR_SIZE
[ TCP ]: Never TSO defer under periods of congestion .
drm / i915 : Simplify i915_gem_obj_is_pinned () test for set - tiling
[ IA64 ] Hole in IA64 TLB flushing from system threads
[ PATCH ] IB / mthca : Fix memory leak on error path
UBI : Fastmap : Notify user in case of an ubi_update_fastmap () failure
blkcg : don ' t call into policy draining if root_blkg is already gone
ARM : OMAP1 : omap h2 regression fix
fealnx : Write outside array bounds
svcrdma : Free context on post_recv error in send_reply
clockevents : export clockevents_unbind_device instead of clockevents_unbind
ixgbe : check for vfs outside of sriov_num_vfs before dereference
ftdi_sio : fix kref leak
b43 : Force - wake queues on init
rt2x00 : disable TKIP on USB
staging : rtl8188eu : Use kcalloc instead of kzalloc .
mm : migrate : hugetlb : putback destination hugepage to active list
um : Missing pipe handling
[ MTD ] [ OneNAND ] omap2 : panic_write may be in an interrupt context
VFIO : platform : clear IRQ_NOAUTOEN when de - assigning the IRQ
drm / exynos / fbdev : set smem_len for fbdev
perfcounters : fix refcounting bug
cfq - iosched : slice offset should take ioprio into account
pxa3xx_nand : reset read buffer before reading
Staging : Android : ion - Fix for memory leak if ion device registration get failed .
ata : fix acpi_bus_get_device () return value check
[ media ] drivers / media / video / davinci / vpbe . c : introduce missing kfree
arm64 : KVM : use ' int ' instead of ' u32 ' for variable ' target ' in kvm_host . h .
NVMe : End sync requests immediately on failure
kobject : free memory if netlink_kernel_create () fails
iwlwifi : mvm : disable uAPSD due to bugs in the firmware
[ media ] rtl820t : Add a debug msg when PLL gets locked
f2fs : avoid unneeded lookup when xattr name length is too long
iwlagn : block regular host commands if driver don ' t own uCode
ext4 : truncate_inode_pages () in orphan cleanup path
drm / i915 : fixup assert_pipe to take the pipe A quirk into account
ide : add missing rq . ref_count initialization to ide_diag_taskfile ()
drm / mgag200 : free memory allocated with alloc_apertures ()
Staging : unisys : Use kcalloc instead of kzalloc to allocate array
f2fs : enable fast symlink by utilizing inline data
ALSA : asihpi - Fix outstream start trigger for non - mmap adapters .
[ ALSA ] hda - codec - Add first generation macbook subsystem ID
spi_mpc8xxx : Fix uninitialized variable
cpufreq : powernv : Free ' chips ' on module exit
hwmon : ( k10temp ) Remove duplicate pci - id define
decnet : fix shutdown parameter checking
HID : debug : fix error handling in hid_debug_events_read ()
fs / pstore : update the backend parameter in pstore module
[ POWERPC ] Make irq_dispose_mapping ( NO_IRQ ) a nop
kgdb : add missing __percpu markup in arch / x86 / kernel / kgdb . c
sd : Clear PS bit before Mode Select .
ASoC : ad1836 : fix a memory leak if another ad1836 is registered
ide : add hwif -> chipset fixup to ide_device_add ()
ALSA : hda - Fix NULL dereference with CONFIG_SND_DYNAMIC_MINORS = n
[ POWERPC ] Fix rtas_flash procfs interface
PCI : host : Add of_pci_get_host_bridge_resources () stub
ath6kl : Fix bug in scheduling hb_timer
[ NETFILTER ]: ip6t_mh : drop piggyback payload packet on MH packets
virtio : fix suspend when using virtio_balloon
vmalloc : remove vmalloc_sync_all () from alloc_vm_area ()
xfrm : Don ' t allow esn with disabled anti replay detection
pinctrl : pinctrl - imx1 - core . c : Cleaning up if unsigned is less than zero
ipv4 : enable getsockopt () for IP_NODEFRAG
perf symbols : Properly align symbol_conf . priv_size
rt2x00 : Don ' t initialize MM40 HT protection to RTS / CTS on PCI devices
dmaengine : omap - dma : Correct status reporting for memcpy
staging : crystalhd : fix a style warning
Staging : agnx : Move a dereference below a NULL test
iommu / vt - d : Fix memory leak in dmar_insert_one_dev_info ()
setup_arg_pages : diagnose excessive argument size
drm / i915 / skl : Add missing SKL ids
staging : drm / omap : allocation fail check
xfs : validate acl count
mac80211 : drop 4 - addr VLAN frames earlier if not connected
[ media ] dvb_frontend : fix compiler warning
usb : xhci : exit early in xhci_setup_device () if we ' re halted or dying
fs / proc / task_mmu . c : check the return value of mpol_to_str ()
mei : mei_irq_thread_write_handler check for overflow
perf bench sched - messaging : Plug memleak
ipv6 : Fix ip6_xmit to send fragments if ipfragok is true
[ GFS2 ] Fix calculation of demote state
mm : numa : Do not account for a hinting fault if we raced
x86 / entry : Fix _TIF_USER_RETURN_NOTIFY check in prepare_exit_to_usermode
USB : atmel_usb_udc : Use kzalloc () to allocate ep structures
[ SCSI ] remove unnecessary scsi_delete_timer () call in scsi_reset_provider ()
pvpanic : Set high notifier priority
staging : lustre : lustre : obdclass : obd_mount . c : Fix NULL dereference
staging : rtl8188eu : Use ODM_REG_IGI_A_11N and ODM_BIT_IGI_11N in odm_DIGInit ()
xfs : fix memory leak in xlog_recover_add_to_trans
mfd : vexpress - sysreg : Initialize ' site ' variable
[ media ] r820t : Fix IF scale
tcp / dccp : fix potential NULL deref in __inet_inherit_port ()
PCI : Fix oops in pci_vpd_truncate
drm / fb - helper : Do the ' max_conn_count ' zero check
[ PATCH ] Enable RAID autorun on Mac partition tables .
greybus : power_supply : do not release failed supply alloc
bnx2x : properly handle CFC DEL in cnic flow
gen_nand : Test if nr_chips field is valid
[ PATCH ] Clear task_struct -> fs_excl on fork ()
proc / sysctl : fix the int overflow for jiffies conversion
staging : rtl8192e : don ' t just print an error and continue
fbcon : Fix bit_putcs () call to kmalloc ( s , GFP_KERNEL )
powercap / RAPL : add IDs for future Xeon CPUs
i2c / i2c - omap : add a const qualifier
usb : musb : core : fix IRQ check
[ media ] cx88 - dsp : Use kmalloc_array () in read_rds_samples ()
pNFS / flexfiles : Fix a statistics gathering imbalance
hfsplus : fix Buffer overflow with a corrupted image
proc : fix the potential use - after - free in first_tid ()
[ media ] s5p - fimc : Use pix_mp for the color format lookup
userns : Check euid no fsuid when establishing an unprivileged uid mapping
KVM : fail KVM_SET_VCPU_EVENTS with invalid exception number
tcmu : return on first Opt parse failure
USB : gadgetfs : Fix a potential memory leak in ' dev_config ()'
[ ARM ] 5072 / 1 : corgi_pm : don ' t register devices on non - corgi machines
ehci - hcd : Fix typo in an error message
HID : cp2112 : fix gpio - callback error handling
staging : ozwpan : Return error , if PD is not connected .
DMAENGINE : at_hdmac : locking fixlet
ARM : S3C64XX : Staticise s3c6400_sysclass
drm / i915 : don ' t reallocate the compressed FB at every frame
lib / mpi / mpicoder . c : looping issue , need stop when equal to zero , found by ' EXTRA_FLAGS =- W '.
ALSA : timer : fix division by zero after SNDRV_TIMER_IOCTL_CONTINUE
[ IPV4 ] fib_trie : fix duplicated route issue
staging : r8188eu : Add files for new drive : Cocci spatch " noderef "
Input : serio_raw - link serio_raw misc device to corresponding
KVM : x86 emulator : refuse SrcMemFAddr ( e . g . LDS ) with register operand
ipc / shm . c : check for integer overflow during shmget .
mac80211 : call dev_alloc_name before copying name to sdata
ALSA : hdspm - Provide ioctl_compat
mips : handle pgtable_page_ctor () fail
USB : Checking the wrong variable in usb_disable_lpm ()
drm / radeon / kms : mmio base / size should be resource_size_t .
dm mpath : fix NULL pointer dereference when path parameters missing
USB : xhci : drop spinlock in xhci_urb_enqueue () error path .
video : ARM CLCD : Add missing error check for devm_kzalloc
skge : Unmask interrupts in case of spurious interrupts
Input : twl4030 - pwrbutton - fix a leak of the IRQ during init failure
perf top : Fix zero or negative refresh delay
drm : Avoid oops in GEM execbuffers with bad arguments .
mac80211 : extend fast - xmit to driver fragmentation
greybus : timesync / pm : Make synchronous call to restore FrameTime
fs : befs : remove unnecessary * befs_sb variable
video : s3c - fb : return an error when bpp is invalid
[ SPARC64 ]: Fix of_device bus_id settings .
hv_netvsc : fix a race between netvsc_send () and netvsc_init_buf ()
fuse : fix bad return value in fuse_file_poll ()
ALSA : snd - usb - caiaq : Lock on stream start / unpause
ocfs2 : Filter - ENOSPC in mlog_errno ()
ds2760_battery : Add missing kfree ( di ) in ds2760_battery_remove ()
perf top : Add top . children config option
mac80211 : simplify AP interface stop
fm10k : don ' t try to stop queues if we ' ve lost hw_addr
mmc : core : fix error path in mmc_host_alloc
USB : cdc - acm . c : Fix compile warnings
rtnl : fix msg size calculation in if_nlmsg_size ()
USB : Add device quirk for Microsoft VX700 webcam
clk : si5351 : Fix clkout rate computation .
arm64 : fix NULL dereference in have_cpu_die ()
crypto : eseqiv - Fix IV generation for sync algorithms
ixgbe : use kzalloc for allocating one thing
[ ALSA ] emu10k1 - Fix STAC9758 front channel
Btrfs : Lower the max inline size to 8k
rrunner . c : fix possible memory leak in rr_init_one ()
mm : vmscan : fix misused nr_reclaimed in shrink_mem_cgroup_zone ()
gpio : aspeed : Add open - source and open - drain support
isdn : avoid copying too long drvid
proc : Point / proc / net at / proc / thread - self / net instead of / proc / self / net
[ SCSI ] lpfc : introduce missing kfree
amd64_edac : simple fix to allow reporting of CECC errors
PCI : use pci_is_root_bus () in pci_read_bridge_bases ()
staging : bcm2835 - camera : prefer allocating memory using sizeof (* ptr ) instead of sizeof ( struct type name )
staging : slicoss : fix a leak when kzalloc fail
cfg80211 : minimal error handling for wext - compat freq scanning
drm / ttm : test for dma_address array allocation failure
perf evlist : Fix max mmap_pages
x86 : watchdog - check for CPU is being supported
int340x_thermal / processor_thermal_device : return failure when
ath : do not update cycle counters with sleep mode
ext4 : error proc entry creation when the fs / ext4 is not correctly created
xfs : fix duplicate message output
V4L : Storage class should be before const qualifier
ixgbe : remove ifdef check for non - existent define
[ media ] media : rcar_vin : Reject videobufs that are too small for current format
be2net : fix pmac_id [] allocation size
drm : Perform ioctl command validation on the stored kernel values
e1000e : increase minimum frame size allowed
mac80211 : Change WARN_ON to WARN_ON_ONCE
drm / i915 : Track the primary plane correctly when reassigning planes
sched / balancing : Fix ' local -> avg_load > sds -> avg_load ' case in calculate_imbalance ()
dm stripe : fix potential for leak in stripe_ctr error path
ceph : reset iov_len when discarding cap release messages
[ media ] ascot2e : Fix I2C message size check
drm / nv40 / mpeg : fix context handling
ocfs2 : Improve rename locking
batman - adv : remove extra negation in gw_out_of_range ()
[ PATCH ] sata_sil24 : kill 10ms sleep in softreset
arm / arm64 : KVM : Don ' t allow creating VCPUs after vgic_initialized
UBI : Silence an unintialized variable warning
tty : an overflow of multiplication in drivers / tty / cyclades . c
gianfar : Do not reuse pages from emergency reserve
ehea : Fix TSO and hw checksums with non - accelerated vlan packets .
p54 : print unknown eeprom fields
drivers / rtc / rtc - snvs . c : remove redundant of_match_ptr
ALSA : hda - Intialize more codec fields in snd_hda_codec_reset ()
iwlagn : overwrite EEPROM chain setting for 6250 devices
PCI : hv : Use the correct buffer size in new_pcichild_device ()
staging : iio : Make write_event_value callback optional
isdn / hisax : fix compiler warning on hisax_pci_tbl
mwifiex : fix corner case association failure
panic , vt : do not force oops output when panic_timeout < 0
[ SCSI ] megaraid : Use the proper type to hold the irq number .
iio : hid - sensors : Prevent crash during hot - unplug
USB : make the autosuspend workqueue thread freezable
Staging : rtl8712 : fix potential null pointer dereference
tty / serial / sirf : fix error propagation in sirfsoc_uart_probe ()
tpm / st33zp24 / spi : Improve st33zp24_spi_evaluate_latency
[ PATCH ] relax ide - cd dma restrictions
x86 : fix broken flush_tlb_others_ipi ()
pcmcia : check for pointer instead of pointer address
crypto : camellia_aesni_avx - Fix CPU feature checks
cciss : fix reporting of max queue depth since init
irq : check domain hwirq range for DT translate
ocfs2 : Re - order iput in ocfs2_drop_dentry_lock
[ media ] tda8290 : Fix a bug if no tuner is detected
drm / i915 : Simplify i915_gem_obj_is_pinned () test for set - tiling
bonding : use rcu_access_pointer () in bonding_show_mii_status ()
drm / exynos : check crtc ' s dpms mode at SetCrtc
[ PATCH ] savagefb : Remove NULL check
drm / i915 / dp : add lane_count check in intel_dp_check_link_status
ARM : mach - imx : clk - pllv2 : Staticize clk_pllv2_ops
tracing : Use TRACE_MAX_PRINT instead of constant
svcrdma : Free context on post_recv error in send_reply
[ PATCH ] hugetlb : check p ? d_present in huge_pte_offset ()
sysctl : Drop reference added by grab_header in proc_sys_readdir
drm / nouveau / i2c / gp104 : initial support
staging : usbip : userspace : Check return value for mkdir ()
Staging : bcm : Remove assignment in if condition reported by checkpatch . pl
perf session : Fix crash with invalid CPU list
fs / ramfs / file - nommu . c : replace count * size kzalloc by kcalloc
TG3 : limit reaches - 1
mtd : blkdevs : fix crash related to sysfs attributes .
dlm : close othercons
tracing : fix ref count in splice pages
slab : convert last use of __FUNCTION__ to __func__
rbd : reduce memory used for rbd_dev fields
dm snapshot : fix metadata corruption
drm / radeon / atom : workaround vbios bug in transmitter table on rs780
lib : list_sort_test (): add extra corruption check
[ PATCH ] USB : trivial DEBUG message correction in gadget ether driver
iio : staging : ad7298_ring : Fix maybe - uninitialized warning
sch : add missing u64 in psched_ratecfg_precompute ()
bnx2x : Change to driver version 1 . 72 . 10 - 0
usb : gadget : bdc_udc : set value for common is_selfpowered
clk : divider : Fix table round up function
ocfs2 : honor O_ ( D ) SYNC flag in fallocate
smc91c92_cs . c : fix bogus compiler warning
x86 / apic : Fix the devicetree build in certain configs
blkcg : fix double free of new_blkg in blkcg_init_queue
pinctrl : s3c24xx : off by one in s3c24xx_eint_init ()
ALSA : pcm : don ' t override timestamp unconditionally
md : fix bug in raid10 recovery .
cyclades : fix read buffer overflow
aoe : error printed 1 too early
ipc / shm . c : check for overflows of shm_tot
mwiflex : avoid possible null pointer dereference
vmwgfx : return - EFAULT if copy_to_user fails
cpufreq : update user_policy .* on success
OMAP : DSS2 : Taal : Fix ESD check
usb : gadget : ci13xxx_pci : guard against devices without driver_data
hwmon : ( lm87 ) Fix vrm write operation
dlm : make posix locks interruptible
staging : rtl8188eu : Use kcalloc instead of kzalloc
ASoC : atmel - pdmic : add missing clk_disable_unprepare () on error in atmel_pdmic_cpu_dai_startup ()
greybus : loopback : Send some sensible data
Btrfs : adjust NULL test
iscsi - target ; Enforce 1024 byte maximum for CHAP_C key value
perf tools : Try to lookup kernel module map before creating one
iwlwifi : mvm : BT coex - fix BT prio for probe requests
libata : add SATA PMP revision information for spec 1 . 2
mwifiex : block data traffic to tx paused receive address
ALSA : hda - Clear left - over hp_pins in snd_hda_parse_pin_def_config ()
perf bench : Fix memcpy benchmark for large sizes
[ S390 ] s390 / setup : set default preferred console device " ttyS "
wan / fsl_ucc_hdlc : remove reduplicative freed memory ' uhdlc_priv '
Staging : hv : channel : export vmbus_close to modules
blk - stat : fix blk_stat_sum () if all samples are batched
ecryptfs : initialize new auth_tokens before teardown
gpio : moxart : Use devm_gpiochip_add_data () for gpio registration
staging : lustre : lnet : make sure lnet data not greater than LIBCFS_IOC_DATA_MAX
Staging : wilc1000 : Replace memset with eth_zero_addr
staging : vt6655 : Use alloc_etherdev () for kzalloc
[ ARM ] 4233 / 1 : nand / s3c2410 . c : warning fix
mfd : ab8500 - debugfs : Apply a check for - ENOMEM after allocating memory for sysfs
clk : hisilicon : Use kcalloc () in hisi_clk_init ()
fix pgoff in " have to relocate " case of mremap ()
cxgb4vf : Use defined Mailbox Timeout
crypto : img - hash - Fix hash request context
ixgbe : Allow FDB entries access to more RAR filters
KVM : PPC : Add emulation for dcba
atm : ambassador : use NULL instead of 0 for pointer
pinctrl : pinctrl_select_state : set the old_state back on error
tty : Deprecate ldisc . chars_in_buffer () method
bridge : provide a mtu () method for fake_dst_ops
mfd : Pass htc - pasic3 led platform data through the cell platform_data
Staging : gs_fpgaboot : Replace ' int32_t ' with ' int '.
ARM : msm : Fix section mismatches in proc_comm . c
HID : logitech - hidpp : add boundary check for name retrieval
ALSA : fm801 : explicitly free IRQ line
i2c : omap : on -> remove () call pm_runtime_put_sync ()
reiserfs : Don ' t call reiserfs_get_acl () with the reiserfs lock
drm / i915 / gvt / kvmgt : remove some dead code
f2fs : set dirty state for filesystem only when updating meta data
gpio : GPIOHANDLE_GET_LINE_VALUES_IOCTL : Fix information leak
mfd : Add free max8998 -> ono irq in max8998_irq_exit ()
Btrfs : Handle transid == 0 while opening devices
USB : idmouse : fix NULL - deref at probe
powerpc / crypto / crc32c - vpmsum : Fix missing preempt_disable ()
[ NETFILTER ]: nfnetlink_queue : fix possible NULL - ptr dereference
[ media ] xc2028 : add missing break to switch
ASoC : Intel : Fix incorrect sizeof () in sst_hsw_stream_get_volume ()
f2fs : avoid gc in cp_error case
drm / i915 : Return stored value from max freq sysfs entry
ASoC : omap - pcm : off - by - one in ' omap_pcm_limit_supported_formats '
vxlan : Update hard_header_len based on lowerdev when instantiating VXLAN
bna : ethtool : Avoid reading past end of buffer
atl1 : zero out CMB and SBM in atl1_free_ring_resources
drm / vmwgfx : Clear an uninitialized struct member
[ media ] soc - camera : change order of removing device
tcp : fix 0 divide in __tcp_select_window ()
x86 , PAT : Changing memtype to WC ensuring no WB alias
drm / radeon : add a dpm quirk for all R7 370 parts
ASoC : tlv320dac33 : Avoid powering off while in BIAS_OFF
usb : gadget : u_ether : fix dereference after null check coverify warning
fix IN_DELETE_SELF on overwriting rename () on ramfs et . al .
scsi : ufs : qcom : Don ' t free resource - managed kmalloc element
sparc64 : Fix overshoot in nid_range ().
ixgbe : Disallow device reset during ethtool test
ch7006 : correctly handling failed allocation
batman - adv : randomize initial seqno to avoid collision
mlx4_core : Fix array overrun in dump_dev_cap_flags ()
[ media ] em28xx - dvb : Prepare for si2157 driver getting more parameters
fuse : fix SEEK_END incorrectness
ASoC : wm8904 - fix memory leaks
mv643xx_eth : potential null dereference
Input : tegra - kbc - fix inverted reset logic
kernel / exit . c : avoid undefined behaviour when calling wait4 ()
drm / vc4 : plane : Expand the lower bits by repeating the higher bits
rsi : release skb if rsi_prepare_beacon fails
ASoC : Free the card ' s DAPM context
mmc : moxart : Handle error from mmc_of_parse ()
irda : smsc wait count reaches - 1
target / iblock : Add checks for backend DIF emulation
drm / nouveau / pci / gp104 : initial support
agp : info leak in agpioc_info_wrap ()
cifs : fix possible memory corruption in CIFSFindNext
staging : goldfish : Call free_irq in error path
acpi : add checking for NULL early param
bus : arm - ccn : Do not attempt to configure XPs for cycle counter
mmc : renesas_sdhi_internal_dmac : add R8A77980 to whitelist
usb : storage : shuttle_usbat : fix discs being detected twice
iwlwifi : pcie : fix TVQM queue ID range check
ALSA : usb - audio : WARN_ON when alts is passed as NULL
USB : CDC ACM : Fix NULL pointer dereference
crypto : eseqiv - Fix IV generation for sync algorithms
[ ARM ] dma : convert IOMD DMA to use sg_next ()
USB : option : reduce interrupt - urb logging verbosity
ARM : 7809 / 1 : perf : fix event validation for software group leaders
drm / i915 : Annotate user relocs with __user
drm / radeon : only apply the SS fractional workaround to RS [ 78 ] 80
iio : lmp91000 : Set parent device
KVM : async_pf : Add missing call for async page present
Staging : Octeon - usb : octeon - hcd . c return value
gma500 : mdfld_dsi_pkg_sender : fix - Wtype - limits warning
vmxnet3 : annotate hwaddr members as __iomem pointers
drm / i915 : get / put runtime PM when we get / put a power domain
f2fs : correct removexattr behavior for null valued extended attribute
[ SCSI ] be2iscsi : fix memory leak on error path
x86 / microcode / intel : Fix memleak in save_microcode_patch ()
KVM : arm / arm64 : timer : Check for properly initialized timer on init
cnic : call cp -> stop_hw () in cnic_start_hw () on allocation failure
drm / nouveau / bios : memset dcb struct to zero before parsing
userfaultfd : hugetlbfs : remove superfluous page unlock in VM_SHARED case
KVM : powerpc : fix init / exit annotation
[ PATCH ] sil24 : add testing for PCI fault
mtd : nand : don ' t free the global data too early
fcaps : clear the same personality flags as suid when fcaps are used
perf symbols : Only un - prelink non - zero symbols
[ media ] imon : fix other RC type protocol support
[ PATCH ] powerpc : Fix bug in bug fix for bug in lmb_alloc ()
floppy : don ' t write kernel - only members to FDRAWCMD ioctl output
mac80211 : fix fragmentation code , particularly for encryption
usb : chipidea : Allow disabling streaming not only in udc mode
net : avoid NULL deref in inet_ctl_sock_destroy ()
i40e : Add missing braces to i40e_dcb_need_reconfig ()
[ PATCH ] mac80211 : fix GCC warning on 64bit platforms
[ media ] dvb - usb . h function rc5_scan
perf sched replay : Support using - f to override perf . data file ownership
ARM : OMAP2 +: make omap_dma_dev_info __initdata
Btrfs : disk - io . c ( open_ctree ): avoid leaks upon allocation failure
perf top : Fix -- delay_secs 0 division by zero
atm : update msg_namelen in vcc_recvmsg ()
mtip32xx : fix potential crash on SEC_ERASE_UNIT
sctp : support sysctl to allow users to use stream interleave
i2c : designware : Use transfer timeout from ioctl I2C_TIMEOUT
ceph : check mapping to determine if FILE_CACHE cap is used
iio : adis16400 : Report pressure channel scale
perf tools : callchain : Fix spurious ' perf report ' warnings : ignore empty callchains
ixgbe : DCB : remove DCB check config
r8a66597 - hcd : use USB_DT_HUB
xfrm_user : validate XFRM_MSG_NEWAE XFRMA_REPLAY_ESN_VAL replay_window
signal : always clear sa_restorer on execve
quota : info leak in quota_getquota ()
crypto : drbg - fix failure of generating multiple of 2 ** 16 bytes
misc : mei : fix stalled read
drm / i915 : Add fifo underrun reporting state to debugfs
KVM : x86 : fix for buffer overflow in handling of MSR_KVM_SYSTEM_TIME ( CVE - 2013 - 1796 )
drm / i915 : fix integer overflow in i915_gem_execbuffer2 ()
llc : Fix missing msg_namelen update in llc_ui_recvmsg ()
xfs : add missing rmap error return
batman - adv : Only write requested number of byte to user buffer
xen : fix memory leak in __xen_pcibk_add_pci_dev ()
KVM : x86 : apply kvmclock offset to guest wall clock time
fs : umount on symlink leaks mnt count
xfs : fix missing error check in xfs_rtfree_range
i386 : arch / x86 / math - emu / fpu_entry . c warning fix
mm : add ' try_get_page ()' helper function
[ CRYPTO ] scatterwalk : Handle zero nbytes in scatterwalk_map_and_copy
drm / i2c : tda998x : Fix memory leak in tda998x_encoder_init error path .
amd - xgbe : Update connection validation for backplane mode
staging : android : Break up a long line in binder_send_failed_reply
crypto : qat - update init_esram for C3xxx dev type
tcp : fix use after free in tcp_xmit_retransmit_queue ()
drm / i915 : Fix out - of - bounds array access in bdw_load_gamma_lut
ASoC : DaVinci : pcm , constrain buffer size to multiple of period
drm : Shift wrap bug in create_in_format_blob ()
ipv6 : fix headroom calculation in udp6_ufo_fragment
wm831x_power : Add missing kfree ( wm831x_power ) in wm831x_power_remove ()
iwlwifi : mvm : fix possible memory leak
ath10k : fix sending wmi cmd during the tdls teardown
bpf : sockmap , duplicates release calls may NULL sk_prot
mac80211 : mesh only parameter mppath maybe unused
tracing / filters : don ' t remove old filters when failed to write subsys -> filter
mtd : spi - nor : improve macronix_quad_enable ()
USB : sierra : fix urb and memory leak in resume error path
Btrfs : fix btrfs_search_slot_for_read backwards iteration
TOMOYO : Fix oops in tomoyo_mount_acl ().
aoe : Fix OOPS after SKB queue changes .
hwmon : ( ina2xx ) don ' t accept shunt values greater than the calibration factor
module : Don ' t report discarded init pages as kernel text .
[ media ] dvbdev : replace kcalloc with kzalloc
tpm / st33zp24 / spi : Improve st33zp24_spi_evaluate_latency
page_cgroup should ignore empty nodes
KVM : Improve create VCPU parameter ( CVE - 2013 - 4587 )
x86 : fix broken flush_tlb_others_ipi ()
cfg80211 : initialize sinfo in cfg80211_get_station
be2iscsi : add an missing goto in error path
PCI : cpqphp : Fix possible null pointer dereference
perf tools : Fix segfault when using srcline sort key
tracing / filters : fix off - by - one bug
staging : octeon : Fix warning of prefer ether_addr_copy .
sfc : insert ARFS filters with replace_equal = true
dlm : close othercons
UBIFS : return EROFS in case of broken commit
[ media ] gspca : reset image_len to 0 on LAST_PACKET when discarding frame
crypto : algif_aead - fix reference counting of null skcipher
staging : comedi : usbduxsigma : remove tidy_up () from (* auto_attach )
Btrfs : fix wrong max_to_defrag in btrfs_defrag_file ()
ipv4 : Do not use dead fib_info entries .
iwlwifi : mvm : reduce active dwell time
sfc : Use kzalloc () to ensure struct efx_spi_device is fully initialised
[ SCSI ] ipr : Always initiate hard reset in kdump kernel
mm : check that we have the right vma in __access_remote_vm ()
mfd : max77687 : Add registration of max77686 - clk
video : wm8505fb : Remove redundant of_match_ptr
pinctrl : imx1 : fix return value check in imx1_pinctrl_core_probe ()
tile : array underflow in setup_maxnodemem ()
ALSA : timer : Fix leak in SNDRV_TIMER_IOCTL_PARAMS
unix : Use FIELD_SIZEOF () in af_unix_init ().
drm / i915 : fix up the relocate_entry refactoring
[ PATCH ] Bluetooth : fix potential NULL ptr deref in dtl1_cs . c :: dtl1_hci_send_frame ()
mtd : nand : mpc5121_nfc : show parent device in sysfs
drivers / staging / cx25821 / cx25821 - video . c : introduce missing kfree
tcp : Set CHECKSUM_UNNECESSARY in tcp_init_nondata_skb
ceph : fix erroneous cap flush to non - auth mds
goldfish_pipe : use GFP_ATOMIC under spin lock
ACPI : ioapic : Clear on - stack resource before using it
mmc : sdhci - st : explicitly request exclusive reset control
kconfig / nconf : prevent segfault on empty menu
sc16is7xx : enable the clock
efi / fb : Correct PCI_STD_RESOURCE_END usage
x86 : x86 CORE_DUMP_USE_REGSET
drm : stop information leak of old kernel stack .
wcn36xx : Fix error handling
[ GFS2 ] gfs2 endianness bug : be16 assigned to be32 field
btrfs : send : silence an integer overflow warning
ARM : 6665 / 1 : Add Snapper9260 rtc wakeup interrupt pin
mac80211 : fix rcu locking
netrom : fix invalid use of sizeof in nr_recvmsg ()
spectra : Don ' t overwrite nand_debug_level . It ' s a module param
drm / atomic : Handle funcs -> best_encoder == NULL case
xen / blkfront : Make sure that the device is fully ready before allowing release .
i40e : keep service tasks out of reset process
ipvs : Reject ipv6 link - local addresses for destinations
perf evlist : Return first evsel for non - sample event on old kernel
xfs : simple btree query range should look right if LE lookup fails
Btrfs : Fix balance panic
usb : gadget : mv_udc : fix readl error
KVM : nSVM : always intercept VMLOAD / VMSAVE when nested ( CVE - 2021 - 3656 )
firewire : ohci : correct signedness of a local variable
cifs : fix off - by - one bug in build_unc_path_to_root
dmaengine : ste_dma40 : off by one in d40_of_probe ()
skge : Adjust a null pointer check in skge_down ()
[ PATCH ] fuse : handle error INIT reply
[ PATCH ] knfsd : Protect update to sn_nrthreads with lock_kernel
[ PATCH ] i2c : chips / ds1374 . c fixup
Input : wm97xx - core - use IRQF_SAMPLE_RANDOM
drm / i915 : Initialise g4x watermarks for disabled pipes
[ media ] ov9650 : off by one in ov965x_enum_frame_sizes ()
cxlflash : off by one bug in cxlflash_show_port_status ()
xfrm : Restrict extended sequence numbers to esp
ucc_geth : Add SUPPORTED_MII and SUPPORTED_Autoneg
ibmvnic : Fix NAPI structures memory leak
Bluetooth : Release BTM while sleeping to avoid deadlock
perf intel - PT / BTS : Add missing initialization
ALSA : HDA : Fix internal microphone on Dell Studio 16 XPS 1645
Staging : sb105x : info leak in mp_get_count ()
mwifiex : using right aid value for tdls action frame
pci : Don ' t crash when reading mpss from root complex
platform / mellanox : mlxreg - hotplug : Fix uninitialized variable
hwmon : ( scmi ) handle absence of few types of sensors
xprtrdma : Fix DMA - API - DEBUG warning by checking dma_map result
[ media ] media - device : fix infoleak in ioctl media_enum_entities ()
brcmfmac : Fix crash on cleanup .
[ media ] tvp5150 : allow get / set_fmt on the video source pad
Btrfs : skip commit transaction if we don ' t have enough pinned bytes
USB : fix dereference in drivers / usb / misc / adutux . c
staging : iio : isl29018 : fix sparse warning regarding incorrect type ( different signedness )
xfrm_user : fix info leak in copy_to_user_tmpl ()
drivers : net : slip : fix NPD bug in sl_tx_timeout ()
mfd : twl6040 : Check for error when reading revision register
USB : serial : console : fix use - after - free on disconnect
ASoC : Intel : Skylake : fix noderef . cocci warnings
drm / i915 / execlists : Add a GEM_TRACE to show when the context is completed
[ XFS ] fix valid but harmless sparse warning
KVM : MTRR : remove MSR 0x2f8
bcache : fix cache_set_flush () NULL pointer dereference on OOM
dmapool : fix overflow condition in pool_find_page ()
rapidio : switch to get_user_pages_fast ()
drm / msm / mdp5 : make helper function static
[ PATCH ] oom : kthread infinite loop fix
ext4 : do not use stripe_width if it is not set
KVM : x86 : Avoid theoretical NULL pointer dereference in kvm_irq_delivery_to_apic_fast ()
ath9k : Configure AR_PHY_SWITCH_COM with apropriate antenna control
ASoC : Instantiate card widgets immediately
xfrm_user : fix info leak in copy_to_user_auth ()
afs : Add missing afs_put_cell ()
ALSA : hda - Fix auto - mic detection in Conexant codec - parser
ext4 : verify extent header depth
rtl8xxxu : prevent leaking urb
iio : st_sensors : make detection more helpful
rpmsg : Only invoke announce_create for rpdev with endpoints
locks : fix list insertion when lock is split in two
USB : ohci_usb can oops on shutdown
ext4 : fix memory leak in xattr
cgroup : fix a leak when percpu_ref_init () fails
gpio : mb86s70 : Return error if requesting an already assigned gpio
Btrfs : fix em leak in find_first_block_group
usb : hub : Don ' t wait for connect state at resume for powered - off ports
KVM : Fix pit memory leak if unable to allocate irq source id
[ SCSI ] iscsi_tcp : update header size during relogin
hwmon : ( w83627hf ) Fix vrm write operation
[ SERIAL ] 8250_early . c passing 0 instead of NULL
tg3 : Disable 5717 serdes and B0 support
wifi : wilc1000 : validate length of IEEE80211_P2P_ATTR_CHANNEL_LIST attribute
ALSA : timer : Fix leak in events via snd_timer_user_ccallback
gpio : omap : add missed spin_unlock_irqrestore in omap_gpio_irq_type
iscsi - target : fix extract_param to handle buffer length corner case
[ PATCH ] Char : mxser_new , correct include file
xfs : fix acl count validation in xfs_acl_from_disk ()
hwmon : ( stts751 ) buffer overrun on wrong chip configuration
xfrm : fix possible null deref in xfrm_init_tempstate
Btrfs : copy string correctly in INO_LOOKUP ioctl
kernel / exit . c : call proc_exit_connector () after exit_state is set
ARM : imx : call imx6sx_cpuidle_init () conditionally for 6sll
perf tools : Disallow PMU events intel_pt and intel_bts until there is support
[ NETFILTER ]: H . 323 helper : fix possible NULL - ptr dereference
iommu / vt - d : Remove warning for HPET scope type
ACPI : Add DMI entry for Sony VGN - FW41E_H
lz4 : ensure length does not wrap
dccp : fix info leak via getsockopt ( DCCP_SOCKOPT_CCID_TX_INFO )
spi : lpspi : fix memory leak in fsl_lpspi_probe
i40e : Fix a sleep - in - atomic bug
usb : musb : blackfin : call gpio_free () on error path in musb_platform_init ()
dm cache : fix leaking of deferred bio prison cells
drm / mgag200 : check alloc_apertures () success in mga_vram_init ()
phy : exynos - mipi - video : fix check on array index
iwl : off by one bug
irda : Fix missing msg_namelen update in irda_recvmsg_dgram ()
mtd : mtd_blkdevs : don ' t increase ' open ' count on error path
compat : fix 4 - byte infoleak via uninitialized struct field
scsi : qla2xxx : Fix system crash for Notify ack timeout handling
sched : debug : update exec_clock only when SCHED_DEBUG
ALSA : emu10k1 - Fix digital / analog switch on audigy2 ZS
ARM : ux500 : set coherent_dma_mask for dma40
xfs : fix directory hash ordering bug
ethtool : time to blink provided in seconds not jiffies
ASoC : simple - card : Add a NULL pointer check in asoc_simple_card_dai_link_of
perf probe : Load map before glob matching
f2fs : handle error case with f2fs_bug_on
[ media ] uvcvideo : Fix race of open and suspend in error case
udp : harden copy_linear_skb ()
printk : use vprintk_func in vprintk ()
hwmon : ( pc87360 ) Fix vrm write operation
ksmbd : fix memory leak in smb2_handle_negotiate
rt2x00 : allow AP and mesh mode to operate simultaneously
kernel / params . c : fix the maximum length in param_get_string
pinctrl / nomadik : use zero as default irq_start
OMAP : HWMOD : Adding clockdomain check
inetpeer : initialize -> redirect_genid in inet_getpeer ()
[ ALSA ] hda - codec : Add two new systems to ALC883
mt76 : fix variable gain adjustment range
drm : restore open_count if drm_setup fails
net_sched : cls_route : remove the right filter from hashtable
ALSA : asihpi : incorrect range check
mac80211_hwsim : fix possible memory leak in hwsim_new_radio_nl ()
x86 : add dummy acpi_numa_processor_affinity_init () implementation on 32 - bit
mutex : Fix missing conditions to build mutex_spin_on_owner ()
f2fs : fix to return correct error number in f2fs_write_begin
Staging : lustre : module : Replace function calls
drm / i915 : fix integer overflow in i915_gem_do_execbuffer ()
mtd : pcmciamtd : remove dead code
ath10k : abort scan properly if wmi_scan_stop fails
ath9k : don ' t run periodic and nf calibation at the same time
xfs : fix kernel memory exposure problems
sched / numa : Fix math underflow in task_tick_numa ()
[ XFS ] Check return value of xfs_buf_get_noaddr ()
ax25 : fix info leak via msg_name in ax25_recvmsg ()
dm log writes : move IO accounting earlier to fix error path
mm : numa : Do not account for a hinting fault if we raced
drm / amd / display : Fix FMT truncation programming
tilegx : Don ' t receive packets when the napi budget == 0
drivers : net : xgene : fix possible use after free
exec : -- cap do not set inheritable capabilities
Fixed possible NULL pointer dereference .
vhost : fix queue number check when setting inflight FD
ocsp : corrected the comparison of the serial size in OCSP response
driver : secure : use consttime_memequal for memory comparison
[ sipmsgops ] fix codec_delete_XX () parsing
Fixed Memory Overflow Errors
Verify that the native handle was created
dm : validate inputs in vq_endchains
OMXCodec : check IMemory :: pointer () before using allocation
LibCrypto : Don ' t copy the prime test candidates
Problem : ZMTP v1 static allocator is needlessly resized
fail HELLO command when SSL is required
Handle one more division by 0 in TFLite .
[ security ] Fix failed shape check in RaggedTensorToVariant .
[ tfg ][ functiondef_import ] Emit error on empty function attributes
* Fixed Github Vulnerability Report GHSL - 2022 - 066
Check the content setting type is valid .