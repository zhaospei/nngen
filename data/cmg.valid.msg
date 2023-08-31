mpeg2dec : Revert ranking switch with ffdec_mpeg2video . Fixes # 574461
daemon : ldm : avoid manual free ()
daemon : parted : Don ' t crash if part - get - name called on non - partitioned device ( RHBZ # 1142158 ).
virt plugin : compare pointer to NULL , not 0
rrdtool plugin : Wait for the ` queue thread ' to exit ..
openssl : fix uninitialized memory access
bb # 11514 - check packSizes prior to dereference
fix dumping of javascript in pdf
fixing small memory leak .
PE : Remove erroneous if - clause
utils / transicc / transicc . c : Fix incorrect logical operator
refs . c : abort ref search if ref array is empty
block - sha1 : make the size member first in the context struct
daemon : unbreak NO_IPV6 build regression
http . c : Remove bad free of static block
git - show : Reject native ref
git - compat - util : make UNLEAK less error - prone
[ PATCH ] Fix count - delta overcounting
Illumos 5311 - traverse_dnode may report success when it should not
imapc : Fix memory leak in disconnected state
lib - imap - client : Don ' t start another DNS lookup if there ' s already one ongoing .
fts - solr : Removed deprecated waitFlush parameter from commit .
Missing error checking in epoll_create (). Patch by Andrey Panin
auth : oauth2 - introspection lookup error should be internal failure
anvil : Penalty tracking was moving last checksums wrongly with memcpy ().
imapc : Added assert to make sure callback is never NULL .
istream - decrypt : Correctly check the header length for v1
lib - storage : mailbox_get_guid () crashed if storage didn ' t support GUIDs .
lib - auth : Fix memory leak in auth_client_request_abort ()
quota : Mark the mailbox deleted before starting to reduce quota .
lmtp : Fixed lmtp_user_concurrency_limit = 0 to actually mean unlimited
imap IDLE : Add also 172 . 168 / 12 to private networks .
authd : always use an empty buffer to read ident reply
dns : make function used only within this unit static
Importing ratbox3 r25213 changeset
fix compiler warning
Fix a g ++ error with -- enable - shell - use - argv
Add a line missed in the merge to 4 . 2 - dev for TRQ - 2058 .
dialog ( k ): safety check for lookup in dialog table at shutdown
Fixed two mem - leaks in Path - handling .
usrloc : re - init sruid struct for each child
mohqueue : remove call from queue if caller does not exist
sca : fix snprintf buffer size for Expires header in replies to SUBSCRIBE .
modules / lcr : print debug message in case from uri regex matching fails
Changed rs_cms_get_transform () to return NULL if CMS is disabled .
Fixed leak in batch_queue_save ().
core : Set mpctx -> chapters to NULL at uninit
dvd : add the last chapter
eq range mapping - 100 .. 100 => 0 .. 100
demux : check whether stream is selected in demux_get_next_pts ()
fix memleaks ; patch by andrew calkin from gmail com
ehh . frame skipping fixed for - ovc frameno
NULLing handle on destroy
Bugfix : Clear total_bad_nonces when zeroing statistics
Added alignment and math macros
Modified various APIs to match our intent .
scsi - disk : fix changeable values for MODE_PAGE_R_W_ERROR
posix - aio - compat : Fix async_conmtext for ioctl
host - libusb : Correct test for USB packet state
Initialize a variable in all cases
vnc : Fix tight_detect_smooth_image () for lossless case
block / cloop : Use g_free instead of free
fix full frame updates for VNC clients
kvm : Drop redundant kvm_enabled from cpu_thread_is_idle
sparc / leon3 . c : fix memory leak
QemuOpts : check NULL input for qemu_opts_del
qemu : msix nit : clear msix_entries_nr on error
tcg - s390 : Icache flush is a no - op .
arm : fix compile on bigendian host
fix loop in cab file processing : ensure window position within frame
bb # 11370 - do not update mirrors on custom database urls
libfdisk : gpt : avoid unnecessary pmbr check
blkid : add DEVNAME = to export output format
sulogin : use EXIT_ *
libblkid / minix : Match minix superblock types
blkid : fix typo in ID_FS_AMBIVALEN =
mount : remove useless if - before - free tests
agetty : fix memory leak [ clang - analyzer ]
libblkid : fix blkid_partlist_devno_to_partition () for extended partitions
swapon : missing separator in swapon - s command
libblkid : don ' t use fstat () in blkid_get_dev_size ()
don ' t close std [ err , out ] after a verify script
don ' t let hdrs [] be sparse
Have to make unspecified - domain be in writable storage .
log quirky searches
lmtpd . c : don ' t try local delivery if mlookup () fails
mailbox : set header size on create so appends work
fix to latency monitor reporting wrong max latency
avio : Free URLContext private data allocated via AVOptions
movdec : Avoid av_malloc ( 0 ) in stss
avplay : free the active audio packet at exit .
gcc bug workaround by ( Jonathan Gray < khalek at linuxgamers dot net >)
h264 : Use the correct argument context in debug log
tta : fix off be 1 error in the end detection .
adding assert to protect delayed_pic
adpcmenc : Do not set coded_frame -> key_frame .
Pad the extradata in the AAC ADTS to AudioSpecificConfig bitstream filter .
rv34 : Avoid NULL dereference on corrupted bitstream
prores : Evaluate all the quantizers
id3v2 : add a mimetype for bmp pictures .
dv : Replace a magic number by sizeof ()
decode h264 end - of - slice flag
lavfi : use designated initializers in avfilter_class
fixing MVs in hq mode
return eof / error if only ffm header has been written , should fix # 815
http : Always allow no - op seek
smoothstreamingenc : Check the output UrlContext before accessing it
Flush the remaning chars in the input buffer after reading the
issue 10 - zmq_strerror problem on Windows
don ' t close a nonexistent connection ( null dereference )
Initialize all of the structure
Removed an unneeded cast .
From David Carne :
GTPv2 : fix Copy - paste error ( CID 1158587 )
ssl_association_remove (): free assoc -> info too
HTTP2 : Fix double free
Qt : add initializers ( CID 1159363 ).
pcapng : Free option_content on error
Fixed icorrect key length after updating constant index
fix double free
fixed php version detection
Fixing possible remote overflow due to mbstring translation .
set default response code to 200
don ' t leak on error
isync with 5 branch
Unterminated paren group in BETHREADS definition of tsrm_tls_set () macro
run : Block setns ()
update - appstream : Don ' t fail badly if remote has not appstream branch
shada : Fix offset saving
getty - generator : fix segfault when $ container_ttys is not set
logind : when enumerating acl devices for seat0 , don ' t set a tag
sd - daemon : verify NOTIFY_SOCKET path length
shared : time - dst : Avoid buffer overflow
resolved : return exit code of event loop
[ PATCH ] extend exec_program []
login : fix mem leak
acpi : make sure we never free an uninitialized pointer
core / cgroup : first print then free
resolved : don ' t allow adding of ANY class / type RRs to local zones
sd - bus : correct size calculation in DBus fd receive
sysctl : parse all keys in a config file
journald : do not free uninitialized pointer in error path
Fix an off by 8
Allow the creation of 48 bit primes
check that ctx is not a null ptr before deref cid # 131
heimdal : Fix the developer O3 build
don ' t leak memory
primitives : renamed NEON detection function
Added assertions to detect invalid call sequences of < transport_free >
libfreerdp - core : fix server - side transport_attach crash
Fixed # 6013 : Check new length is > 0
Added assistance file name NULL check .
Fixed # 6011 : Bounds check in rdp_read_font_capability_set
codec : Another realloc fix
inclode strerror in recv error message MINOR
increase timeout on killing
fix BSONObj ownership
Remove CINT remnant
setForcedLangOptions -> adjust .
fix warning on MacOS X .
Added missing default initializer for fDirector
avcodec / mimic : Fix runtime error : left shift of negative value - 1
dshow : avoid integer overflow when using curbufsize
ffplay : fix type of time_diff in waveform display
avformat / yuv4mpegdec : fix " warning : field_order may be used uninitialized in this function "
avcodec / mpegvideo_enc : Fix possibly exploitable security issue with - bf
jv demuxer : prevent video packet size overflow
h264 : reset first_field when current_picture_ptr is reset
r3d : fix an invalid read introduced in 6bf4c1d
avcodec / aacdec_fixed : Fix undefined shift
rtmppkt : Make pkt -> data reallocable
avformat : unref packet after storing it in internal packet queue
avutil / opt : dont crash on av_opt_set_dict () with NULL
avformat / cinedec : Fix DoS due to lack of eof check
wmalosslessdec : make MCLMS arrays big enough for what is written into them .
lpc : Remove false positive compilation warning about weight being
lavfi : set sample_rate in the default allocator .
mxfdec : fix memleak on mxf_read_close ()
v410dec : Check for sufficient input data . Fixes crash
rv10 : Forward error from rv10_decode_packet
j2kdec : make curtilenum integer .
avcodec / avpacket : Use av_free_packet () in error cleanups
postproc : fix null pointer dereference with invalid option strings
pcx : properly pad the scanline
libavformat / rtpdec_asf : zero initialize the AVIOContext struct
avcodec / h264_mvpred : Fix multiple runtime error : left shift of negative value
hlsenc : Allocate enough space for the pattern string
s3tc : fix shift overflow by using unsigned constant
avcodec / scpr : Check y in first line loop in decompress_i ()
wavpack : check ch_offset
avformat / avidec : Remove ancient assert
asfdec : check stream_index for validity
id3v2 : prevent unsigned integer overflow in ff_id3v2_parse ()
indeo4 : check band -> scan
Fix a crash when probing img2 format with a NULL filename .
avcodec / hevc : clear HEVClc when its deallocated in hevc_decode_free ()
fix incorrect check for abs_diff_pic_num overflow
avcodec / pixlet : Fix reading invalid numbers of bits
mjpegdec : Properly set the context colorspace info
avformat / rmdec : Do not pass mime type in rm_read_multi () to ff_rm_read_mdpr_codecdata ()
perform sanity check on sample rate in electronicarts demuxer
aasc : check before reading the first 4 byte , fix overread
avcodec / extract_extradata_bsf : Fix leak discovered via fuzzing
avformat / mov : Change the type of the r / g / b variables
tests / api / api - seek - test : Silence compiler warnings about uninitialized variables
lavc / mjpegenc : check av_frame_alloc () failure .
avformat / mpegts : fix iteration count in add_pid_to_pmt ()
avformat / nistspheredec : initialize header_size to - 1
avcodec / webp : Update canvas size in vp8_lossy_decode_frame () as in vp8_lossless_decode_frame ()
h264 : dont stop parsing NALs without cleanup on DPC .
h265_parse : skip zero sized NAL units
avcodec / mjpegdec : make type of shift unsigned to avoid undefined behavior
vf_fps : Fix memory leak introduced by eea64ef4
wmalosslessdec : Reset residues to 0 if they were not decoded from bitstream .
av_find_stream_info (): set subtitle codec time_base
avconv : Replace goto redo on decode fail with continue .
avcodec / vc1 : reset fcm / field_mode in non advanced header parsing
hls : Free packets when skipping packets when seeking
vc1 : check image height , fix division by 0
avformat / oggparseogm : Check ff_alloc_extradata () for failure
wavpackenc : assert on too small buffer
avcodec / mlpdec : Fix runtime error : left shift of negative value - 1
dds : disable palette flag for compressed dds
lavf / cafenc : Only allow mono and stereo for Opus .
rtpdec : Fix the minimum packet length for RTCP SR packets
lavd / lavfi : fix compiler warning for uninitialized variables
RTMP : fix FD leak in rtmp_open ()
Check for out of bound writes in the avs demuxer .
cast size to int64_t to avoid overflow
ffserver : free pathname before bailing out
tools / target_dec_fuzzer : Fix return code on open failure
mov : Fix near infinite loop in stsd parsing .
vorbisdec : check codebook entry count
avcodec / pngdec : mark previous_picture as done on end of decode_frame_common ()
avformat / mpegtsenc : Changed Video PES packet length to 0 .
libx264 : do not set pic quality if no frame is output
ff_h264_direct_ref_list_init : fix B slice check .
lavc : use av_mallocz to allocate AVFrames .
avcodec / xsubdec : Check that RLE coded image and colors fit in the buffer
avformat / mvdec : Use 64bit for ret to avoid overflow
j2kdec : fix division by zero , check tile dimensions for validity
bintext : fix possible x variable overflow .
omadec : Properly check lengths before incrementing the position
vaapi_h264 : Set max_num_ref_frames to 1 when not using B frames
avidec : fix " avidec . c : 362 : warning : st may be used uninitialized in this function "
avcodec / movtextdec : Fix potential integer overflow
jpeg2000 : check len before parsing header
flacdec : Silence false positive warning about uninitialized variables in
avio : check AVIOContext malloc failure
binkaudio : fix channel count check
avformat / img2dec : zero probe buffer
zmbv : Reset the decoder on keyframe errors
wmapro : error out on impossible scale factor offsets
hevc : use av_mallocz () for allocating tab_ipm
avcodec / dcaadpcm : fix use of uninitialized variable
mpeg12dec : reset data size after parsing extradata .
postproc / postprocess : Check for av_mallocz () failure
lavfi / mp : fix invalid read with filters with no argument .
avformat / omadec : only compute timestamps based on bitrate if its set
avcodec / hevc : Dont turn 32bit timebases into negative numbers
tools / target_dec_fuzzer : Remove FuzzerInterface . h dependancy
v410enc : fix output buffer size check
lavf / http : Remove superfluous parenthesis .
jpeg2000 : Check that nreslevels2decode has been initialized before use
avcodec / tiff : Fix use of uninitialized off variable
rle_code can overflow when multiplied by 4
h264 : prevent rbsp_buffer values from becoming duplicated
avformat / svag : fix division by zero
ffv1 : make sure gob_count is not 0
mpegaudiodec : fix short_start calculation
tta : fix off be 1 error in the end detection .
avutil / internal : Do not enable CHECKED with DEBUG
electronicarts : check timebase , fix assertion failure
fix mem leak
avcodec / cbs_jpeg : Check length for SOS
avformat / mpc8 : fix hang with fuzzed file
avplay : Do not print a possibly uninitialized value
avcodec / shorten : Check residual size
avformat / mpegenc : Better heuristic for ordering packets
avformat / mov : Fix signed integer overflows with total_size
shorten : pad the internal bitstream buffer
sws : fix assert failure
h264 : check for integer overflow , fix null pointer dereference
avcodec / mpegvideo : make mc_mb_var_sum_temp / mb_var_sum_temp 64bit
vorbisdec : fix heap buffer overflow .
avcodec / aac : fix () in IS_CODEBOOK_UNSIGNED macro
avcodec / h264_slice : don ' t sync default_ref [] between threads .
wmadec : Verify bitstream size makes sense before calling init_get_bits .
h264 : correct ref count check and limit , fix out of array accesses .
ffplay : only update pts if not redisplaying a frame
avcodec / cinepak : fix integer underflow
lavfi : fix use - after - free in ff_filter_frame .
fixing mem corruption
fix mem leak
avformat / mxfdec : Fix memleak of packages_refs
avcodec / dvdsubdec : error on bitmaps with size 0
avcodec / ffv1dec : Fix out of array read in slice counting
examples / demuxing : free AVPacket after usage
qdm2 : fix signed multiplication overflow
ffmpeg : Clear error message array at init .
avf_showwaves : fix null pointer dereference
svq1dec : dont export the qscale table .
avutil / hwcontext_dxva2 : return an error when buffer allocation fails
avcodec / snowdec : Check qbias
avcodec / mpeg4videodec : Clear mcsel before decoding an image
mxg : fix compiler warning for uninitialized variables
cbs_h265 : Fix reading of unknown parameter set extension data
svq3 : Avoid a division by zero
h264 : Mark previous field as done before throwing it away .
jpeg2000 : zero reslevel array on allocation
ac3dsp : fix loop condition in ac3_update_bap_counts_c ()
rtpdec_h264 : Free old extradata before clearing the pointer
aacdec / lcg_random : fix harmless integer overflow
avcodec / h264_slice : Fix signed integer overflow
avcodec / hevc_cabac : Check prefix so as to avoid invalid shifts in coeff_abs_level_remaining_decode ()
msmpeg4enc : fix possible use of uninitialized warning
vf_pad : check that we have write permission before writing in the buffer .
Fixed invalid access in wavpack decoder on corrupted extra bits sub - blocks .
ffmpeg : avoid NULL dereference with filters .
libopusenc : check return value
ffv1 : check for malloc failure .
srtp : Mark a few variables as uninitialized
avcodec / ff_h264_decode_sei : fix integer overflow with size .
ac3dec : apply dynamic range compression to correct channels in dual - mono mode .
libx265 : Fix use of uninitialized input picture
aac_parser : add required padding for GetBitContext buffer
vc2enc : increase the starting value of the size scaler
avcodec / g2meet : check available space before copying palette
Fix a mem leak in av_find_stream_info ().
hnm4 : change width / height to int to fix hypothetical integer overflows
quickdraw : Skip the empty 512 byte header for images
lavf probe : prevent codec probe with no data at all seen
lavf / utils : fix mem leak
fix initial vbv buffer underflow
avprobe : Zero the allocated avio buffer memory
avcodec / cllc : Check bitstream end before decoding pixels
avformat / rmdec : check against mismatching int4 interleaver parameters which would leave uninitialized holes
mpegenc : prevent a NULL pointer dereference
motionpixels : Prevent calling init_vlc () with invalid parameters
h264 : don ' t initialize missing pictures when using a hwaccel
vp9 : make mv bounds 32bit .
avcodec / wmalosslessdec : fix mclms_coeffs * array size
sdp : Check that fmt -> oformat is non - null before accessing it
matroskadec : fix out of bounds write
xan : Prevent NULL dereferences with missing reference frame
drawtext : draw glyphs for the strftime ()- expanded text
avformat / ipmovie : Check that the OPCODE_INIT_AUDIO_BUFFERS size is large enough
ffmpeg : Check read_ffserver_streams () return value
applehttp : fix variant discard logic
fix mem leak
avformat / mov : clear padding area in mov_read_extradata ()
mlpdec : dont leave a invalid huff_lsb in the context .
avcodec / mmvideo : Fix undefined behavior ( left shift of negative value )
avcodec / libxavs : Use av_mallocz_array ()
opt : check memory allocation
avutil / opt : ensure the right buffer is used in set_string_number ()
avutil / hwcontext_d3d11va : Fix leak when wrapping texture in AVD3D11FrameDescriptor
graphparser : add a NULL check on the argument passed to strstr
hdsenc : Avoid integer overflow
g726 : Do not try to decode more than one channel .
mpegaudiodec : Enable checked bitstream reader .
h264 : fix clang warning about uninitialized variable
avoid possibly uninitialized return value
mss3 : prevent AC state from becoming invalid in rac_normalise ()
get_bits : change the failure condition in init_get_bits
avutil / eval : check for av_malloc failure
audio_frame_queue : fix null ptr dereference
avformat / aviobuf : Fix signed integer overflow in avio_seek ()
avcodec / cfhd : Fix undefined shift
avcodec / bitstream : assert that no integer overflow happened when writing codes in build_table ()
rtsp : Free the rtpdec context properly
vaapi_encode : Discard output buffer if picture submission fails
error concealment : check last pictures motion_val before using it .
lavc / libschroedinger : add missing failure checks in ff_create_schro_frame ()
sdp : Fix null pointer dereference with aac and ffserver .
libfdk - aacdec : Correct buffer_size parameter
dvenc : dont fail hard if the timecode is invalid
libavformat : Check for malloc failures in avformat_new_stream
libx265 : Fix ' braces around scalar initializer ' warning
libtheora : Check frame allocation
avcodec : disallow reget_buffer () if pix_fmt changed .
avformat / mov : fix memory leak
avconv : initialize hwaccel_pix_fmt
avcodec / sbrdsp_fixed : Fix integer overflow
qsvdec : Fix memory leak
avformat / librtmp : Fix memory leak if RTMP_ConnectStream () fails
avconv : check that the output format context exists before accessing it
swscale : fix NULL checking in sws_alloc_context ()
libavutil / opencl : fix potential null dereference
avformat / mvdec : Fix DoS due to lack of eof check
check for malloc failure
indeo3 : Fix overreading requant_tab .
avcodec / asvdec : Use rounded up dimenensions in input size check
mjpegbdec : dont return a picture when there is no picture .
zerocodec : check if there is previous frame
indeo5 : Fix null pointer dereference .
ffv1enc : Fix memleak
vc2enc : fix segfault
oggdec : fix invalid free on error
avcodec / apedec : tmpk == 32 is not supported , prevent undefined behavior
avpacket : Initialize the allocated padding area in side data
examples / filtering_audio : fix frame leak .
avcodec / dpx : fix framerate
avformat / oggparseopus : Check that granule pos is within the supported range
tests / api / api - codec - param - test : Do not directly access caps_internal
mpc8 : Check out of bound bands limit
handle http error codes
aac : Relax reserved_bit validation
http : Don ' t use the normal http proxy mechanism for https
Fix a small memory leak
mpegvideo : synchronize AVFrame pointers in ERContext fully
bink : Check for out of bound writes when building tree
bail out early , skip snprintf , if buflen is 0 ; ok cloder
add LOG entry when opening feed in browser
hw / intc / slavio_intctl : Avoid shifting left into sign bit
target - sparc : on UA2005 don ' t deliver Interrupt_level_n IRQs in hypervisor mode
target - i386 : Fix jmp im on x86_64 when executing 32 - bit code
linux - user : Check dump_write () return in elf_core_dump ()
virtio : avoid buffer overrun on incoming migration
linux - user : fix broken cpu_copy ()
vmxnet3 : Do not fill stats if device is inactive
target - i386 : fix protection bits in the TLB for SMEP
esp - pci : fixup deadlock with linux
target - mips : fix EntryHi . EHINV being cleared on TLB exception
block : Fix memory leak in hmp_drive_add_node ()
vnc : fix segfault
hw / arm : Mark the " fsl , imx31 " device with user_creatable = false
tests : fix e1000 - test leak
block / mirror : fix broken sparseness detection
usb : fix use after free
vnc : fix a qio - channel leak
dma - helpers : Initialize DMAAIOCB in_cancel flag
tests : fix bios - tables - test leak
ahci : advertise HOST_CAP_64
linux - user / signal . c : PPC : Silence clang uninitialized - use warning
ppc : Remove a potential overflow in muldiv64 ()
vhost - user : fix watcher need be removed when vhost - user hotplug
lm32 : fix build breakage due to uninitialized variable ' r '
e500 : fix memory leak
device_tree : Fix potential memory leak
pvpanic : use get_uint () for " ioport " property
qemu - ga : don ' t leak a file descriptor upon failed lockf
sysbus : fix memory leak
cirrus : don ' t overflow CirrusVGAState -> cirrus_bltbuf
ipmi_bmc_sim : Fix off by one in check .
vnc : clear vs -> tlscreds after unparenting it
esp : fix coding standards
rdma : Fix qemu crash when IPv6 address is used for migration
hw / nvram / spapr_nvram : Device can not be created by the users
vnc : fix crash when vnc_server_info_get has an error
audio : release capture buffers
s390x : Limit s390 - ccw machines to 248 CPUs
hid : Reset kbd modifiers on reset
qdev : Fix use after free in qdev_init_nofail error path
qemu - char : Fix crash when switching consoles
linux - user - i386 : Fix crash on cpuid
i386 : kvmvapic : initialise imm32 variable
vvfat : fix qemu - img map and qemu - img convert
ui : Make qemu_default_pixman_format () return 0 on unsupported formats
target / openrisc : Fix writes to interrupt mask register
target - mips : Correct the handling of register # 72 on writes
VMXNET3 : initialize rx_ridx to eliminate compile warning
libqos : remove some leaks
s390x / mmu : Fix the exception codes for illegal table entries
x86 : translate . c : remove dead assignment
tpm : Fix write to file descriptor function
ps2 : reset queue in ps2_reset_keyboard
pc_sysfw : Plug memory leak on pc_fw_add_pflash_drv () error path
x86 / cpuid : Tighten parsing of tsc_freq = FREQ
gtk : skip keyboard grab when hover autograb is active
target - i386 : Fix x86_cpuid_set_model_id ()
dt : temporarily disable subtree creation failure check
exec . c : Fix off - by - one error in register_subpage
linux - user : pause () should not pause if signal pending
qxl : locking fix
json - streamer : Don ' t leak tokens on incomplete parse
spapr_drc : Expose ' null ' in qom - get when there is no fdt
block : Limit request size ( CVE - 2014 - 0143 )
hw / pl011 . c : Avoid crash on read when no chr backend present
replay : fix error message
target - i386 : svm : Fix MSRPM check
block : clean up bdrv_drain_all () throttling comments
qcow2 : Fix memory leak in COW error path
dma : xlnx - zynq - devcfg : Fix up XLNX_ZYNQ_DEVCFG_R_MAX
qcow2 : Assert that the crypto header does not overlap other metadata
pci / shpc : fix signed integer overflow
qdev : Fix crash when using non - device class name on - global
qcow2 : Fix refcount table size calculation
hw / vmmouse . c : Disable vmmouse after reboot
virtio - 9p - proxy : Fix sockfd leak
virtio : optimize virtio_access_is_big_endian () for little - endian targets
blkverify : Fix leak of opts in blkverify_open
s390x / tcg : handle WAIT PSWs during interrupt injection
throttle : Remove block from group on hot - unplug
spapr : initialize local Error pointer
block / drive - mirror : Check for NULL backing_hd
crypto : fix stack - buffer - overflow error
qga : fix false negative argument passing
PPC : Fix TLB invalidation bug within the PPC interrupt handler .
bitmap . h : Don ' t include qemu - common . h
json - parser : Fix potential NULL pointer segfault
block : Fix multiwrite memory leak in error case
tcg / ppc : Do not overwrite lower address word on Darwin and AIX
vvfat : fix a file descriptor leak
rtc - test : skip year - 2038 overflow check in case time_t is 32bit only
sdl : Do not make full screen mode resizable
spapr : sanity check size of the CAS buffer
linux - user : Fix locking order in fork_start ()
fix race between timer firing vs . alarm_timer -> pending = 0
qemu - ga : for w32 , fix leaked handle ov . hEvent in ga_channel_write ()
block / ssh : Avoid segfault if inet_connect doesn ' t set errno .
kvm : Add kvm system event crash handler
usb : sanity check setup_index + setup_len in post_load
vnc : sanitize bits_per_pixel from the client
pc - dimm : error out if memory hotplug is not enabled
e1000e : correctly tear down MSI - X memory regions
slirp : fix guestfwd id
vhost - scsi : fix k -> set_guest_notifiers () NULL dereference
pseries : Correct panic behaviour for pseries machine type
qdev : fix use - after - free in the error path of qdev_init_nofail
exec . c : fix dirty bitmap reallocation
qcow2 : fix condition in is_zero_cluster
hw / ppc / spapr_iommu : Fix crash when removing the " spapr - tce - table " device
usb - ehci : drop assert ()
qcow2 : Don ' t open images with header . refcount_table_clusters == 0
usb : ehci : fix memory leak in ehci_init_transfer
pcie : fix improper use of negative value
qcow2 : fix leak of Qcow2DiscardRegion in update_refcount_discard
exec : fix ram_list dirty map optimization
hw / intc / xilinx_intc : Avoid shifting left into sign bit
target - i386 : CPUID : return highest basic leaf if eax > cpuid_xlevel
usb - storage : fix NULL pointer dereference .
target - arm : fix for exponent comparison in recpe_f64
block : avoid useless checks on acb -> bh
kvmvapic : Disable if there is insufficient memory
check for bs -> drv in bdrv_flush ( Christoph Hellwig )
ide : ahci : unparent children buses before freeing their memory
spapr : Move memory hotplug to RTAS_LOG_V6_HP_ID_DRC_COUNT type
hw / pci : fix crash on shpc error flow
qga - win32 : Fix memory leak of device information set
target / ppc : Fix wrong number of UAMR register
block / iscsi : fix memory corruption on iscsi resize
slirp : fix leak
sd : do not add one sector to the disk size
vhost - user - test : fix predictable filename on tmpfs
vmdk : fix double free
parallels : drop check that bdrv_truncate () is working
hw / intc / arm_gic : Fix NVIC assertion failure
target - ppc : Fix use of uninitialized TCG variable in tlbiva
check malloc return for code in rabin2 . c
Fix a memleak
Corrected a condition on the first op
Fixing the breakpoint callback signature . (# 10998 )
Oops for ' dk '
Remove a now pointless assert .
idle controlled teams serialize as " human " controlled
fixed bug where entering a chat message while the message is selected ...
Update the GUI when using [ replace_schedule ]
use a list for the standard message receiving sound
Initialize all members .
made it so AI players have an empty description in multiplayer games
locations not on the map should instantly fail to location filter
fix ReadOK macro to avoid casting warnings
lxc_spawn : initialize netpipe
sd - dhcp : check for udp packets
fix inotify to work not only once
sysusers : fix memleak (# 4430 )
systemctl : never check inhibitors if - H or - M are used (# 3781 )
fstab - generator : fix trivial leak
sd - bus : correct size calculation in DBus fd receive
Call all configured exec_dump_fn ' s when SIGUSR2 is received
totemcrypto : fix hmac key initialization
Prevent jumps to last page when autoscrolling
[ presentation ] Fix transition of pages with duration = 0
app : silence a harmless critical warning
plug - ins : fix memory leak in edge - neon
Bug 663582 - Brush outline is too slow
app : fix rectangle select tool cursor after committing / halting
branches / zip : lock_update_discard (): Fix a bug introduced in r949 :
fix breakage on qnx
sync0arr . c : Fix compilation error on some systems by introducing all
branches / zip : Check for autoinc next value overflow explicitly when offset
fix a build problem . addresses # 1396
fix for bug # 212
[ trunk ] Import commit 8ec8321df613986e8642e2975f7182072eb4be62 from ghostpdl
src / bin / jpwl / convert . c : fix memleak ( fix suggested by maddin200 , # 631 )
[ trunk ] Fix simple warning about sign conversion
fix duplicate dns_cancel_pending_resolve reported by Giorgos Pallas
Oct 19 13 : 27 : 24 . 391 [ info ] router_reload_networkstatus (): Skipping cached - status file with unexpected name "."
Added missing unlock for pre - mature return from log_msg . - Jette
move slurm_crypto_fini () to prevent memory leak on shutdown
fix uninitialized text outline width in error image
[ runtime ] Fix minor leak
Replace a g_assert with a g_error .
* mini - s390 [ x ]. c ( is_regsize_var ): Support PTR / FNPTR too .
geoipbackend : Avoid expensive destroy during initialization
toolbar : plug a couple of memory leaks
Added # prog_id import syntax for TSs without SDT
fixed gcc warning
Fixed wrong AU seq num len when hinting (# 640 )
don ' t try to dump null boxes (# 1093 )
atr table reallocation fix by William Wanders
remove pointer to freed secret object
fixed timer order .
bzero when close new_fd .
Added Buffer member
Ignore this commit . * jedi mind trick *
Fix a bug with / SHUN deletion ( it didn ' t work )
m_spanningtree Ensure that TreeSocket :: Close () only runs once
regexp_release : allow NULL regexp
* src / syntax . c ( print_tree ): print something for a NULL tree
check ` mrb_open ` return value
Fix an off - by - one causing a buffer overflow in mirb .
Fixed dereference to null pointer in ' codegen . c ' reported by ' clang - analyzer '
reduce invoking mrb_intern from convert_type
ikev2 : reject SKF payloads when the remote never negotiated them
silence a coverity warning
Fixes an issue processing the X - forwarded - for header . Bug # 924 :
x11 : add 0 < ctrls -> numGroups <= 4 assertion
Use 16pix icons for size < 19
Moved check for https :// github . com / ImageMagick / ImageMagick / issues / 92 .
fix memory leak in ReadYCBCRImage as SetImageExtent failure
Fixed memory leak .
fix another memory leak
http : fix leaking of response_code_line
unix : NULL pipe_fname in uv__pipe_close ()
X3F : check for data offset limit
server : http_parser : fix pointer protocol length
iov : buf to free non - zero allocation
bin : declare missing server ctx
http_heade fix , check first that we have a connp before checking transactions
lib / http : add missing whitespace in verbose output
fix compiler warning
http2 : abort the send_callback if not setup yet
urlglob : avoid NULL pointer dereference
curl_easy_reset : Also reset the authentication state
[ SP ] Fixed incorrect cgame syscall argument handling for Menu_Paint .
[ SP ] Win32 : Fix game incorrectly reporting low mem
[ MP ] Fix an incorrect memcpy count argument in CQuickSpriteSystem :: Add .
[ MP ] Fix crash when drawing input fields
string . c : fix integer overflow
m_capab : fix a possible remote crash triggered by the CAPAB parsing code .
Windows : terminate more aggressively when you close UnrealIRCd .
fix error from commit 13585f15c7f7dc28bbbba1661efb280d530d114c
fix bug introduced in 33293
return correct NID for undefined object
RT4002 : check for NULL cipher in p12_crpt . c
set auto ecdh parameter selction for Suite B
make sure we don ' t write to seed [- 1 ]
Free a BIO_ADDR if DTLSv1_listen return <= 0
Bugfix : Free filename entry for writing config file when done with it
libsepol : sepol_av_to_string : clear static buffer
Fixed one byte stack overflow in mcast recvfrom .
libtcmu : free ctx in tcmulib_initialize
trivial : close inotify_fd on exec
Delete the TransferManager on disconnect
src / wavlike . c : Prevent infinite loop on malformed file
src / aiff . c : Validate channel count .
cli : remove superfluous problem_data_free () call
fix crash on unknown shape - reported by Ryan Schmidt
IEEE 802 . 11 : Add a bounds check
print - dhcp6 . c : add TCHECK (* dh6o ) in dhcp6opt_print ()
Fetch a 32 - bit big - endian quantity with EXTRACT_32BITS ().
Check if auth_user is set .
tag server dirty if changing vars
added missing g_free ()
don ' t echo query params on invalid requests to redirect URI ; closes # 212
Fixing compile error from MSVC . Try 2 .
fix : prefix 0x67 missed .
x86 : instruction length must be <= 15
msg : MDentryLink has a private destructor
include / ceph_hash . cc : reduce scope of a var in ceph_str_hash_rjenkins ()
librbd : properly handle replay of snap remove RPC message
filestore : verify that op_seq is nonzero
lockep : fix leak in failure / assert path
librados : fix unsigned vs . signed warning
mon : crush straw_calc_version value is 0 or 1 not 0 to 2 .
rgw : add xml output header in RGWCopyObj_ObjStore_S3 response msg
rgw / rgw_rest . cc : fix uninitialized static struct
rgw : fix a typo
OutputDataSocket : fix uninit var
auth : fix NULL pointer access when trying to delete CryptoAESKeyHandler instance
mds : add IFLOCK to wait bits to prevent collisions with lock branch
librados : Allow rados_pool_list () to get NULL buffer if len == 0
librados : fix use without NULL check in rados_pool_list
Exit the max size loop when there ' s nothing left possible to find
fix memory leak .
set_fat (): Fix off - by - 2 error leading to corruption in FAT12
Reduced the max memory use and image size in 64 - bit builds
Fixed a memory leak in the BMP decoder
LibVNCClient : remove now - useless cast
hfs : fix keylen check in hfs_cat_traverse ()
Added missing DBUG_ENTER ( bug in last push )
branches / zip : buf_page_init_for_read (): Increment the buf_fix_count
add NULL ctx argument check to wolfSSL_X509_STORE_CTX_set_time ()
discard session chain in SSL_clear
Btrfs : add tree block level sanity check
[ PATCH ] tpm : fix memory leak
x86 : properly initialize temp insn buffer for paravirt patching
module : printk message when module signature fail taints kernel .
rmap : recompute pgoff for unmapping huge page
quota : info leak in quota_getquota ()
gpio : palmas : fix a possible NULL dereference
ASoC : OMAP : HDMI : Initialize IEC - 60958 channel status word
cgroup : make sure memcg margin is 0 when over limit
iwlagn : check beacon frame size
drm / i915 : Only clear the GPU domains upon a successful finish
[ IB ] mthca : fix off by one in clr_int calculation
mm : node_states : introduce N_MEMORY
kexec / uefi : copy secure_boot flag in boot params across kexec reboot
iwlwifi : mvm : BT Coex - fix shared antenna check with new API
cxlflash : off by one bug in cxlflash_show_port_status ()
ASoC : wm9713 : fix regmap free path
fix a race in ks8695_poll
qeth : Clear SBALF15 in any case for output buffers .
USB : wusb : check CHID is all zeros before stopping the host
[ PATCH ] rcu : Avoid kthread_stop on invalid pointer if rcutorture reader startup fails
ASoC : Intel : fix missing mutex
iwlwifi : fix situation in which debug message is printed
bsg : fix bsg_poll () to return POLLOUT properly
[ SCSI ] ps3rom : sector size should be 512 bytes
m68k : serial167 missing return value in cy_put_char ()
ath10k : don ' t call quiet mode if it ' s not implemented
tg3 : Disable 5717 serdes and B0 support
md : fix bug in handling of new_data_offset
vfs : do_last (): only return EISDIR for O_CREAT
spi_bfin : use more useful GPIO labels
pch_phub : Fix MAC address writing issue for LAPIS ML7831
fb_defio : Push file_update_time () into fb_deferred_io_mkwrite ()
KVM : SVM : Set the ' busy ' flag of the TR selector
vxge : Fix a possible memory leak in vxge_hw_device_initialize ().
sky2 : Call dev_kfree_skb_any instead of dev_kfree_skb .
mac80211 : send control port protocol frames to the VO queue
KVM : x86 : apply kvmclock offset to guest wall clock time
cifs : ensure that vol -> username is not NULL before running strlen on it
tipc : fix out of bounds indexing
[ S390 ] dasd : Use default recovery for SNSS requests
ARM : 7803 / 1 : Fix deadlock scenario with smp_send_stop ()
ath5k : fix NULL pointer in antenna configuration
net : make ipv6 bind honour freebind
[ PATCH ] enable booting a NUMA system where some nodes have no memory
tty / serial : at91 : correct buffer size used in DMA
staging : ozwpan : Increase interrupt end point buffer size
[ PATCH ] KVM : fix bogus pagefault on writable pages
sfc : Clear I2C adapter structure in falcon_remove_nic ()
ALSA : snd - usb : Fix uninitialized variable usage
[ S390 ] fix initialization of stp
ALSA : hda - Don ' t actually write registers for caps overwrites
perf : Fix inherited events vs . tracepoint filters
x86 , vm86 : clean up invalid_vm86_irq ()
stmmac : fix max coal timer parameter
cfg80211 : enter psm when working as p2p_cli
firewire : fix ioctl () return code
drivers / serial / uartlite . c : Add missing of_node_put
[ SCSI ] mptfusion : Set fw_events_off to 1 at driver load time .
ARM : dma - api : fix off - by - one error in __dma_supported ()
[ PATCH ] USB : fix gadget_is_musbhdrc ()
arm64 / BUG : Show explicit backtrace for WARNs
twl - regulator : Fix reg_disable functionality for 4030 and 6030
ALSA : usb - audio : Fix format string mismatch in mixer . c
mac80211 : initialize power mode for mesh STAs
libceph : init event -> node in ceph_osdc_create_event ()
mwifiex : corrections in PCIe event skb handling
igb : reset PHY after recovering from PHY power down
dm raid1 : switch rh_in_sync to blocking in do_reads
mfd : wm8994 : Silence bogus warning in wm8994_device_init ()
sky2 : use roundup () macro
KVM : ensure that memslot userspace addresses are page - aligned
ALSA : sb16 - info leak in snd_sb_csp_ioctl ()
Drivers : hv : Fix a memory leak
sh : intc : Allocate subgroup virq backing desc directly .
cifs : fix off - by - one bug in build_unc_path_to_root
drm / ttm : Clear the ghost cpu_writers flag on ttm_buffer_object_transfer .
igbvf : clear buffer_info -> dma after dma_unmap_single ()
GFS2 : write_end error path fails to unlock transaction lock
mwifiex : use maximum ssid length as 0xfe for p2p
vfs : fix spinning prevention in prune_icache_sb
rt2x00 : fix a wrong parameter for __test_and_clear_bit () in rt2x00rfkill_free ().
clk : make sure parent is not NULL in clk_fetch_parent_index ()
Phonet : fix double free in GPRS outbound packet error path
thermal : return an error on failure to register thermal class
ASoC : wm8731 : Check for clk_prepare_enable () error
usb : phy : ab8500 - usb : fix phy tuning value select logic
pwm : omap - dmtimer : Potential NULL dereference on error
ASoC : fsi : fixup pm_runtime_disable () timing on fsi_probe ()
drm / radeon / kms : fix typo in spread spectrum code
gpio : delete unneeded test before of_node_put
[ PATCH ] readahead : reset cache_hit earlier
rt2800usb : initialize H2M_INT_SRC register
phy : fix phy address bug
ipv4 : warn once on passing AF_INET6 socket to ip_recv_error
sound : fix check for return value in snd_pcm_hw_refine
GFS2 : quotas not being refreshed in gfs2_adjust_quota
gro : Fix bogus gso_size on the first fraglist entry
md : add takeover support for raid4 -> raid5 conversion .
drm : crtc : integer overflow in drm_property_create_blob ()
Btrfs : fix crash when starting transaction
[ IA64 - SGI ] Do not request DMA memory for BTE
[ media ] ir - lirc - codec . c : don ' t leak lirc -> drv - rbuf
ath10k : make sure to use passive scan when n_ssids is 0
Btrfs : ignore non - FS inodes for send / receive
mmc : sdhci - bcm - kona : Handle error from mmc_of_parse ()
drm / vmwgfx : Fix nasty write past alloced memory area
[ IPV6 ] ADDRCONF : Register inet6_dev earlier .
staging : brcm80211 : remove unnecessary cast in wlc_d11hdrs_mac80211
md : skip all metadata update processing when using external metadata .
batman - adv : Fix mem leak in the batadv_tt_local_event () function
usb : core : check for valid id_table when using the RefId feature
[ X25 ]: Restore skb -> dev setting in x25_type_trans ().
ASoC : wm5110 : Correct DSP4R Mixer control name
mfd : Copy the device pointer to the twl4030 - madc structure
[ SCSI ] virtio_scsi : fix memory leak on full queue condition .
crypto : scatterwalk - Avoid flush_dcache_page on slab pages
i2o : fix overflow of copy_to_user ()
xhci : Handle HS bulk / ctrl endpoints that don ' t NAK .
gpio : potential null dereference
x86 : tsc_64 . c make constant UL
iio : Accept a leading '+' sign when parsing fixed point numbers
drm / i915 : Simplify i915_gem_obj_is_pinned () test for set - tiling
powerpc / ipic : introduce missing kfree
[ PATCH ] savagefb : Fix kfree before use
Btrfs : make error return negative in btrfs_sync_file ()
sound / soc / fsl / p1022_ds . c : add missing of_node_put
ARM : OMAP1 : omap h2 regression fix
perf bench : Fix NULL pointer dereference in " perf bench all "
staging : rtl8188eu : Use kcalloc instead of kzalloc .
staging : vt6656 : don ' t return zero on failure path in vt6656_probe ()
[ media ] media - device : fix infoleak in ioctl media_enum_entities ()
i40e : Increase PF reset max loop limit
ipw2200 : support ETHTOOL_GPERMADDR
brcmfmac : Fix crash on cleanup .
usb : phy : mv - otg : use to_delayed_work instead of cast
perf : Fix error return code
[ SCSI ] aacraid : fix potential panic in thread stop
chipidea : error on overflow for port_test_write
radeon_display : Use pointer return error codes
drm / i915 : Missed lock change with rps lock
omap_hsmmc : add missing probe handler hook
leds : Make header variable naming consistent
[ IA64 ] Use set_cpus_allowed_ptr
Btrfs : check return value of btrfs_search_slot properly
Drivers : hv : hv_balloon : correctly handle val . freeram < num_pages case
drm / i915 : Add a couple WARN ()' s to catch missing locks
ASoC : Check return value of strict_strtoul () in WM8962
AFS : Potential null dereference
kobject : free memory if netlink_kernel_create () fails
[ XFS ] Fix a mutex_destroy diagnostic about a locked - mutex - on - destroy from
vmwgfx : return - EFAULT if copy_to_user fails
irqchip : irq - crossbar : Not allocating enough memory
staging : comedi : ni_660x : use comedi_handle_events ()
headers_check fix : linux / bfs_fs . h
Btrfs : free tmp ulist for qgroup rescan
drm / i915 : WARN if the bios reserved range is bigger than stolen size
ide : add missing rq . ref_count initialization to ide_diag_taskfile ()
mm , percpu : Make sure percpu_alloc early parameter has an argument
drm / mgag200 : free memory allocated with alloc_apertures ()
pinctrl : baytrail : add missing module removal support
f2fs : enable fast symlink by utilizing inline data
ab8500 - fg : Use correct battery charge full design
ASoC : rt5640 : add default case for unexpected ID
drm / i915 : Skip stolen region initialisation if none is reserved
leds : leds - cobalt - raq . c - use resource_size ()
amd8111e : Don ' t receive packets when the napi budget == 0
ath9k : skip ATH9K_INT_TIM_TIMER when we are idle
drivers / rtc / rtc - pcf2123 . c : fix error return code in pcf2123_probe ()
fs / pstore : update the backend parameter in pstore module
[ S390 ] missing check in dasd_eer_open .
ocfs2 : check if cluster name exists before deref
spi : mediatek : remove clk_disable_unprepare ()
mm , hugetlb : add VM_NORESERVE check in vma_has_reserves ()
drm / nouveau : remove pointless assignment
Input : twl4030_keypad - pass correct pointer to free_irq ()
staging : netlogic : Fix multiple assignments
staging / rdma / hfi1 : Don ' t attempt to qualify or tune loopback plugs
drm / nouveau / pm : don ' t read / write beyond end of stack buffer
[ NETFILTER ]: ip6t_mh : drop piggyback payload packet on MH packets
drm / i915 : make sure panel is sequenced off when starting a mode set
PCI : Allow a NULL " parent " pointer in pci_bus_assign_domain_nr ()
PCI : Mark 64 - bit resource as IORESOURCE_UNSET if we only support 32 - bit
xfrm : Don ' t allow esn with disabled anti replay detection
ixgbe : Convert the normal transmit complete path to dev_consume_skb_any ()
dmaengine : ste : use DMA_COMPLETE for dma completion status
staging : lustre : prevent underflows in lnet_parse_reply ()
qeth : keep ip - address after LAN_OFFLINE failure
drivers / staging / cx25821 / cx25821 - video . c : introduce missing kfree
ext4 : remove dynamic array size in ext4_chksum ()
dmaengine : omap - dma : Correct status reporting for memcpy
x86 : fix warning on 32 - bit non - PAE
KVM : PIT : control word is write - only
USB : fix regression occurring during device removal
[ ALSA ] hda - codec - Fix NULL dereference in generic hda code
[ SCSI ] mptfusion : Fix memory leak in mptctl_getiocinfo ()
[ PATCH ] fix creating zero sized bio mempools in low memory system
[ media ] uvcvideo : Fix a " ignoring return value of ‘ __clear_user ’" warning
ALSA : make sparc / dbri . c : snd_dbri_proc () static
ocfs2 : unmap_mapping_range () in ocfs2_truncate ()
[ media ] media : omap3isp : fix a potential NULL deref
ceph : fix rare potential cap leak
iget : use iget_failed () in GFS2
setup_arg_pages : diagnose excessive argument size
efivarfs : Never return ENOENT from firmware
xfs : suppress spurious uninitialised var warning in xfs_bmapi ()
[ media ] pctv452e : Replace memset with eth_zero_addr
xfs : catch invalid negative blknos in _xfs_buf_find ()
[ TG3 ]: Fix PHY loopback on 5700
mei : mei_irq_thread_write_handler check for overflow
perf bench sched - messaging : Plug memleak
ipv6 mcast : Check address family of gf_group in getsockopt ( MS_FILTER ).
drivers / w1 / w1_int . c : multiple masters used same init_name
Btrfs : stop leaking btrfs_bios on readahead
[ media ] DVB : Set scanmask for Budget / SAA7146 cards
tracepoints : use rcu_ * _sched_notrace
perf ui / tui : Print helpline message as is
[ PATCH ] e1000 : narrow down the scope of the tipg timer tweak
pvpanic : Set high notifier priority
cfq - iosched : fix no - idle preemption logic
drm / i915 : take struct_mutex in i915_dma_cleanup ()
ARM : LPC32xx : serial . c : Fixed loop limit
at86rf230 : add force slotted operation bit
uvesafb : bitwise OR has higher precedence than ?:
mfd : vexpress - sysreg : Initialize ' site ' variable
qlcnic : Fix potential NULL dereference
tcp / dccp : fix potential NULL deref in __inet_inherit_port ()
cfg80211 : fix elapsed_jiffies calculation
[ PATCH ] tpm : fix memory leak
drm / fb - helper : Do the ' max_conn_count ' zero check
ASoC : fix uninitialised variable in siu_dai . c
powercap / RAPL : add IDs for future Xeon CPUs
[ ALSA ] Don ' t reject O_RDWR at opening PCM OSS with read / write - only device
pNFS / flexfiles : Fix a statistics gathering imbalance
ath9k : Fix a crash in 2 WIRE btcoex chipsets
airo : return from set_wep_key () when key length is zero
wl12xx : protect wl12xx_op_set_rts_threshold ()
drm : shut up invalid edid messages
Btrfs : fix regression of btrfs device replace
[ media ] s5p - fimc : Use pix_mp for the color format lookup
tulip : Properly check dma mapping result
staging / rtl8192u - check for skb alloc failure
handle recursive calls to bust_spinlocks ()
[ ARM ] 5072 / 1 : corgi_pm : don ' t register devices on non - corgi machines
isdn : guard against a potential NULL pointer dereference in old_capi_manufacturer ()
ocfs2_dlm : Missing get / put lockres in dlm_run_purge_lockres
libertas : down_interruptible () can return - EINTR , not EINTR
rsi : avoid format string leak to thread name
ASoC : arizona : Record FLL setting when disabling
selinux : fix off - by - one in setprocattr
staging : ozwpan : Return error , if PD is not connected .
drm / i915 : Use MLC ( l3 $) for context objects
drm / radeon / kms : prefer high post dividers in legacy pll algo
gpio : f7188x : fix edit mistake
drm / i915 : Fix intel_tile_width () parameters
fsl / fman : double free on probe failure
DMAENGINE : at_hdmac : locking fixlet
drivers / net / wan : remove exceptional & on function name
telephony : fix return value
drm / radeon / kms : although LVDS might be possible on crtc 1 don ' t do it .
greybus : call put_device () on error
mac80211 : reduce the amount of unnecessary traffic on cooked monitor interfaces
cxlflash : Fix to prevent stale AFU RRQ
drm / ttm : fix incorrect logic in ttm_bo_io path
drivers / rtc / rtc - ds1307 . c : fix BUG shown with lock debugging enabled
x86 : e820 memmap - add checking for NULL early param
printk : suppress empty continuation lines
[ JFFS2 ] Add declaration of jffs2_lzo_ { init , exit } to compr . h
KVM : cleanup "* new . rmap " type
[ ARM ] pxa : fix pxa27x_udc default pullup GPIO
[ IPV4 ] fib_trie : fix duplicated route issue
drm / i915 : Kill leftover GTIIR writes from valleyview_irq_preinstall ()
[ media ] au8522_common : add missing MODULE_LICENSE
sfc : SFT9001 : Fix condition for LNPGA power - off
ath9k : fix 5 / 10MHz channel width initialization on ar9003
perf probe : Trivial typo fix for -- demangle
timers , drivers / tty / metag_da : Initialize the poll timer as pinned
s390 / chsc : fix ioctl CHSC_INFO_CU command
cxl : Disable SPAP register when freeing SPA
wext : verify buffer size for SIOCSIWENCODEEXT
[ media ] lgdt3306a : one bit fields should be unsigned
[ media ] media : vb2 - memops : Export vb2_get_vma symbol
HID : egalax : Use kzalloc
KVM : x86 emulator : refuse SrcMemFAddr ( e . g . LDS ) with register operand
ipc / shm . c : check for integer overflow during shmget .
[ SCSI ] Fix sense key MEDIUM ERROR processing and retry
mac80211 : call dev_alloc_name before copying name to sdata
irda : Correctly clean up self -> ias_obj on irda_bind () failure .
mips : handle pgtable_page_ctor () fail
wlcore : increase max roc duration to 30 seconds
ARM : 7010 / 1 : mm : fix invalid loop for poison_init_mem
video : ARM CLCD : Add missing error check for devm_kzalloc
perf top : Fix zero or negative refresh delay
[ PATCH ] Signedness issue in drivers / net / phy / phy_device . c
cfg80211 : fix truncated IEs
xfrm_user : fix info leak in copy_to_user_policy ()
iwlwifi : range checking issue
usb : wusbcore : read actual_length bytes isoc in segments
KVM : PPC : e500 : Use kmalloc_array () in kvm_vcpu_ioctl_config_tlb ()
drm / radeon : fix endian bug in radeon_atom_get_mclk_range_table ()
lm8333 : Fix check ordering
[ PATCH ] utsns : fix ! CONFIG_UTS_NS behavior
KVM : PIT : free irq source id in handling error path
[ PATCH ] v9fs : replace strlen on newly allocated by __getname buffers to PATH_MAX
Btrfs : skip adding an acl attribute if we don ' t have to
p54 : eliminate warning for uninitialized variable ' tim_len '
backlight : Fix backlight limiting on spitz / corgi devices
iwlwifi : fix TX tracer
powerpc / eeh : Skip finding bus until after failure reporting
b43 : Fix warning at drivers / mmc / core / core . c : 237 in mmc_wait_for_cmd
[ PATCH ] mempolicy . c GFP fix
cfg80211 : fix channel setting for wext
[ media ] stb0899 : don ' t go past DiSEqC msg buffer
misc : apds9802als : Fix the logic checking timeout in als_wait_for_data_ready ()
crypto : ahash - ensure statesize is non - zero
ext4 : avoid exposure of stale data in ext4_punch_hole ()
[ PATCH ] drivers / message / i2o / pci . c : fix a NULL pointer dereference
ieee1394 : sbp2 : fix probing of some DVD - ROM / RWs
[ PATCH ] uml : fix sizeof usage
drm / radeon / kms : Fix oops after radeon_cs_parser_init () failure .
i2c : robotfuzz - osif : Constify osif_table
[ PATCH ] m68knommu : switch 68360 to using rtc_time
[ SCSI ] zfcp : Fix error checking for ELS ADISC requests
drm / i915 : Allow full PPGTT with param override
staging : bcm2835 - camera : prefer allocating memory using sizeof (* ptr ) instead of sizeof ( struct type name )
[ S390 ] cio : allow setting boxed devices offline
Staging : iio : Remove line over 80 characters
libertas : don ' t clear priv -> dnld_sent after sending sleep confirm
tcp : fix tcp_cong_avoid_ai () credit accumulation bug with decreases in w
clk : hisilicon : Use devm_kmalloc_array () in hisi_clk_alloc ()
cxl : remove redundant increment of hwirq
[ ARM ] pxa / csb726 : register ac97 controller
drm / radeon : fix a bug in the SA code
staging : lustre : dne : setdirstripe should fail if not supported
staging : rts_pstor : Complete scanning_done variable
[ NETFILTER ]: nf_log : add netfilter gcc printf format checking
s390 / scm_blk : fix memleak in init function
scsi_dh : return SCSI_DH_NOTCONN in scsi_dh_activate ()
rndis_wlan : add return value validation
be2net : Bug fix to avoid soft lockup in loopback test .
iwlwifi : mvm : initialise ADD_STA before sending it to the firmware
ALSA : hda - check NULL pointer when creating SPDIF controls
ravb : Add missing free_irq () call to ravb_close ()
drm / i915 : Fix up some stray to_i915 ( dev ) after a recent merge
[ PATCH ] hdaps : use ENODEV
spi : spidev : fix possible NULL dereference
rt2x00 : Shortcut link state updates when not operating as STA
staging / lustre / llite : Fix integer overflow in ll_fid2path
drm / nv40 / mpeg : fix context handling
ux500 : use _cansleep GPIO functions
[ S390 ] hwsampler : allow cpu hotplug
macvtap : advance iov iterator when needed in macvtap_put_user ()
ehea : Fix TSO and hw checksums with non - accelerated vlan packets .
mfd : Change tunnelcreek watchdog name on the lpc_sch subdevices array
p54 : print unknown eeprom fields
uwb : Fix handling IEs with empty IE data in uwb_est_get_size ()
misc : mic : Fix for double fetch security bug in VOP driver
iio : hid - sensors : Prevent crash during hot - unplug
f2fs : allocate new segment aligned with sections
ASoC : pcm3168a : Call clk_set_rate in pcm3168a_set_dai_sysclk
cxgb4 : fix memory leak of qe on error exit path
x86 / apic : Use x2apic physical mode based on FADT setting
drivers / staging / lustre : Fix set - but - unused whinge .
drivers / net / hamradio : Integer overflow in hdlcdrv_ioctl ()
[ ALSA ] Return error if no user TLV is defined
cciss : fix reporting of max queue depth since init
[ media ] lirc_igorplugusb : Fix a compilation waring
irq : check domain hwirq range for DT translate
mfd : Check t7l66 platform_data pointer
dmaengine : omap - dma : Add more debug information when freeing channel
ARM : Fix build error for SMP = n builds
Btrfs : don ' t loop in allocator for space cache
USB : missing error check in emi26
staging : usbip : userspace : Check return value for mkdir ()
b43 : Force - wake queues on init
wl12xx : fix a memory leak of probereq template upon recovery
spi : sh - msiof : Fix warnings due to improper casts
staging : wilc1000 : remove spaces around '->'
slab : convert last use of __FUNCTION__ to __func__
f2fs : initialize newly allocated dnode structure
drm / amd / amdgpu / amdgpu_ttm : Handle return NULL error from ioremap_nocache
xfrm : Ignore socket policies when rebuilding hash tables
drm / i915 : switch off backlight for backlight class 0 brightness
x86 / boot : Fix minor fd leakage in tools / relocs . c
[ PATCH ] USB : trivial DEBUG message correction in gadget ether driver
sound : oxygen : zero - initialize model data
sch : add missing u64 in psched_ratecfg_precompute ()
ARM : shmobile : armadillo800eva : set clock rates before timer init
Staging : rtl8192e : Fix potential NULL pointer dereference
smc91c92_cs . c : fix bogus compiler warning
[ ARM ] 4056 / 1 : iop13xx : fix resource . end off - by - one in flash setup
x86 / apic : Fix the devicetree build in certain configs
[ PATCH ] x86 - 64 : add memcpy / memset prototypes
samples / bpf : Reset global variables
cpuset : fix RCU lockdep splat in cpuset_print_task_mems_allowed ()
dm stats : fix a leaked s -> histogram_boundaries array
mac80211 : fix txq queue related crashes
mwiflex : avoid possible null pointer dereference
cpufreq : update user_policy .* on success
vhost : actually track log eventfd file
dlm : fix possible use - after - free
usb : gadget : ci13xxx_pci : guard against devices without driver_data
drivers / usb / misc / auerswald : fix status check , remove redundant check
AUDIT : Fix user pointer deref thinko in sys_socketcall ().
uml : drivers / net_user . c memory leak fix
[ PATCH ] NPTL signal delivery deadlock fix
iscsi - target ; Enforce 1024 byte maximum for CHAP_C key value
brcmfmac : check return from kzalloc in brcmf_fweh_process_event
drm / i915 / skl : Ensure pipes with changed wms get added to the state
[ SCSI ] ibmvfc : Fix invalid error response handling
tty : xuartps : Fix " may be used uninitialized " build warning
ASoC : Do not write to invalid registers on the wm9712 .
i40evf : do not re - arm watchdog after remove
[ SCSI ] advansys : Fix bug in AdvLoadMicrocode
sysfs : sysfs_write_file () writes zero terminated data
pti : PTI semantics fix in pti_tty_cleanup .
staging : lustre : lnet : make sure lnet data not greater than LIBCFS_IOC_DATA_MAX
x86 : put trigger in to detect mismatched apic versions
[ media ] cx231xx : add a missing break statement
ASoC : Fix incorrect kfree in wm8731_probe error path
ASoC : ad1836 : fix a memory leak if another ad1836 is registered
ALSA : timer : Fix leak in SNDRV_TIMER_IOCTL_PARAMS
rxrpc : rxrpc_kernel_send_data () needs to handle failed call better
ixgbe : Allow FDB entries access to more RAR filters
pty : fix data loss when stopped (^ S /^ Q )
ipv6 : exit early in addrconf_notify () if IPv6 is disabled
tracing : instance_rmdir () leaks ftrace_event_file -> filter
[ GFS2 ] Plug an unlikely leak
xfs : extra semi - colon breaks a condition
tracing : Fix off - by - one in trace_get_user ()
sky2 : only enable Vaux if capable of wakeup
dma : edma : Fix memory leak
mfd : sta2x11 - mfd : Only add sta2x11_mfd if it hasn ' t already been added
[ PATCH ] powerpc : make OF interrupt tree parsing more strict
Btrfs : fix memory leak in btrfs_parse_early_options ()
UBI : return on error in rename_volumes ()
drivers / net / wireless / b43 / main . c : 4351 : Fixed coding style
mtg : docg3 : potential divide by zero in doc_write_oob ()
mfd : Add free max8998 -> ono irq in max8998_irq_exit ()
sfc : check for allocation failure
regmap : Silence warning on invalid zero length read
leds : add LED_ON brightness as boolean value
gro : Reset dev pointer on reuse
ARM : ensure vm_struct has its phys_addr member filled in
md : check for memory allocation failure in faulty personality
[ NETFILTER ]: nfnetlink_queue : fix possible NULL - ptr dereference
slub : Zero initial memory segment for kmem_cache and kmem_cache_node
ALSA : hdsp - info leak in snd_hdsp_hwdep_ioctl ()
quota : Fix WARN_ON in lookup_one_len
ibm_newemac : Fix dangerous struct assumption
dm log writes : move IO accounting earlier to fix error path
igb : use mac loopback for i354 backplane
drm / i915 : Return stored value from max freq sysfs entry
ext4 : verify extent header depth
igb : fix stats for i210 rx_fifo_errors
[ IRDA ]: Fix memory leak in irttp_init ()
staging : unisys : Add checks for creation
devm_memremap : Fix error value when memremap failed
ath9k_hw : fix an off - by - one error in the PDADC boundaries calculation
Staging : comedi : rtd520 : &&/|| typo
tcp : fix 0 divide in __tcp_select_window ()
dm thin : fix a memory leak when passing discard bio down
Btrfs : stop leaking btrfs_bios on readahead
fix IN_DELETE_SELF on overwriting rename () on ramfs et . al .
cpuset : use trialcs -> mems_allowed as a temp variable
staging : lustre : llite : fixup return value ll_direct_IO_26
net / 9p / protocol . c : Fix a memory leak
crypto : talitos - Fix timing leak in ESP ICV verification
cpuidle , menu : use interactivity_req to disable polling
xHCI : add XHCI_RESET_ON_RESUME quirk for VIA xHCI host
drm / i915 : Add lock on drm_helper_resume_force_mode
[ media ] usbvision : fix locking error
ARM : S3C64XX : Reduce residency requirement for cpuidle WFI mode
trace : introduce missing mutex_unlock ()
video : vt8500lcdfb : Remove kfree call since devm_kzalloc () is used
HID : cp2112 : fix gpio - callback error handling
eCryptfs : Unlock page in write_begin error path
ibm_newemac : Fixes entry of short packets
cdrom : fix improper type cast , which can leat to information leak .
rsi : release skb if rsi_prepare_beacon fails
btrfs : send : remove BUG from process_all_refs
iio : mxs - lradc : fix buffer overflow
mmc : moxart : Handle error from mmc_of_parse ()
mac80211 : reduce stack usage in sta_ht_capa_read ()
qla2xxx : Wait for reset completion without lock for ISPFX00 .
drm / nouveau / pci / gp104 : initial support
staging : hv : fix counting of available buffer slots when send fails
usb : musb : host : fix for musb_start_urb Oops
mmc : renesas_sdhi_internal_dmac : add R8A77980 to whitelist
cdrom : Fix info leak / OOB read in cdrom_ioctl_drive_status
mwifiex : corrections in PCIe event skb handling
kernel / signal . c : avoid undefined behaviour in kill_something_info
[ S390 ] hw_random : allow rng_dev_read () to return hardware errors .
rt2x00 : rt2800pci : use rt2x00dev -> tx -> limit
rtl8xxxu : Temporarily disable 8192eu device init
drm / i915 : Clear crtc atomic flags at beginning of transaction
staging : unisys : visorbus : visorchipset . c : Fix bug in parser_init_byte_stream .
fscrypto : add authorization check for setting encryption policy
drm / nouveau / bios : memset dcb struct to zero before parsing
signals , ptrace , sched : Fix a misaligned load inside ptrace_attach ()
staging : olpc_dcon : actually return the value of i2c_add_driver
HID : corsair : fix mapping of non - keyboard usages
mx3fb : fix regression with uninitalized fb_info -> mm_lock mutex
[ ARM ] prevent the vmalloc cmdline argument from eating all memory
pinctrl : s3c24xx : off by one in s3c24xx_eint_init ()
clk : gpio : Unlock mutex on error path
f2fs : avoid wrong error during recovery
floppy : silence warning during disk test
e1000e : NVM write protect access removed from SPT HW
aio : fix kernel memory disclosure in io_getevents () introduced in v3 . 10
ALSA : hdsp : allow firmware loading from inside the kernel
mm : compaction : do not compact pgdat for order - 0
[ media ] imon : fix other RC type protocol support
ALSA : ice1724 - Add PCI postint to reset sequence
staging : slicoss : Fix possible reuse of freed memory in timer function
drm / vc4 : Fix an integer overflow in temporary allocation layout .
[ libata ] ahci : Fix lack of command retry after a success error handler .
staging : rtl8192u : check dev_alloc_skb () return value in fw_download_code ()
KVM : x86 : Don ' t report guest userspace emulation error to userspace
mmc : meson - gx - mmc : Fix platform_get_irq ' s error checking
xen / blkback : Update to use blkdev_get_by_dev instead of open_by_devnum .
mtd_blkdevs : Set the maximum discards size
xtensa : prevent arbitrary read in ptrace
[ media ] dvb - usb . h function rc5_scan
driver core : fix device_create () error path
slub : Use NUMA_NO_NODE in get_partial
ARM : OMAP2 +: make omap_dma_dev_info __initdata
ALSA : hda - Another Dell headset quirk
vfio - pci : Handle error from pci_iomap
staging : typec : tcpm : Check cc status before entering SRC_TRY_DEBOUCE
scsi : sg : allocate with __GFP_ZERO in sg_build_indirect ()
hwmon : Register thermal zone only if ' dev ' parameter was provided
libceph : must hold mutex for reset_changed_osds ()
drivers : hamradio : 6pack : fix UAF bug caused by mod_timer ()
[ SCSI ] mpt2sas : Bump version 08 . 100 . 00 . 00
[ POWERPC ] Improve robustness of the UIC cascade handler
af_key : add __GFP_ZERO flag for compose_sadb_supported in function pfkey_register
powerpc / tm : Flush TM only if CPU has TM feature
iwlwifi : fix compilation warning when CONFIG_IWLWIFI_DEBUG is not set .
xfrm_user : validate XFRM_MSG_NEWAE XFRMA_REPLAY_ESN_VAL replay_window
[ PATCH ] aoe 5 / 12 : don ' t try to free null bufpool
watchdog : imx2_wdt : also set wdog -> timeout to new_timeout
ASoC : topology : Fix memory leak in widget creation
drm / i915 : Correctly reject invalid flags for wait_ioctl
ASoC : soc - core : use GFP_KERNEL flag for kmalloc in snd_soc_cnew
staging : comedi : ni_labpc_cs : remove thisboard macro
[ PATCH ] swsusp : prevent disks from spinning down and up
misc : mei : fix stalled read
drm / i915 : Only flush fbc on sw when fbc is enabled .
[ SCSI ] ipr : dlpar failed when adding an adapter back
Input : gpio_keys - fix a memory leak
drivers / net / phy / mdio_bus . c : call put_device on device_register () failure
dm thin : restore requested ' error_if_no_space ' setting on OODS to WRITE transition
skd : Use kmem_cache_free
xfs : fix freeing memory in xfs_getbmap ()
[ PATCH ] drivers cris : return on NULL dev_alloc_skb ()
[ PATCH ] KVM : Add missing ' break '
[ PATCH ] nfsd4 : renew lease on seqid modifying operations
drm / i2c : tda998x : Fix memory leak in tda998x_encoder_init error path .
ARM64 : PCI : Fix struct acpi_pci_root_ops allocation failure path
tg3 : Fix tg3_skb_error_unmap ()
tcp : fix use after free in tcp_xmit_retransmit_queue ()
perf : arm64 : Record the user - mode PC in the call chain .
USB : usbaudio : handle kcalloc failure
[ PATCH ] x86_64 : free_bootmem_node needs __pa in allocate_aperture
ipv6 : fix headroom calculation in udp6_ufo_fragment
xhci : check for failed dma pool allocation
Btrfs : truncate pages from clone ioctl target range
bpf : sockmap , duplicates release calls may NULL sk_prot
rtc : allow validated RTC_PIE_ON for non - root
video : fbdev : pxa3xx - gcu : Fix integer overflow in pxa3xx_gcu_write
phy : exynos - mipi - video : avoid uninitialized variable use
mtd : spi - nor : improve macronix_quad_enable ()
mfd : don ' t use memzero
usb : gadget : s3c2410_udc : Remove unneeded condition
ASoC : AMD : Add NULL pointer check
usb : dwc3 : gadget : add a debugging print when initializing endpoints
mm : Introduce kvcalloc ()
Btrfs : add missing compression property remove in btrfs_ioctl_setflags
ASoC : cs42l52 : microphone bias is controlled by IFACE_CTL2 register .
i2c - omap : Fix build breaking typo cpu_is_omap_2430
[ XFS ] Fix up warning for xfs_vn_listxatt ' s call of list_one_attr () with
staging : lustre : llite : default dir stripe index only for mkdir
ASoC : qcom : ipq806x : add error in dma allocation .
mfd : palmas : Initialise client -> of_node for dummy created client
AFS : Fix afs_prepare_write ()
tcp : ipv6 : bind () use stronger condition for bind_conflict
staging : iio : iio_ring_rip_outer return immediately if rip_lots returns <= 0
Btrfs : fix wrong max_to_defrag in btrfs_defrag_file ()
greybus : core : add defensive connection disable post disconnect
ipv4 : Do not use dead fib_info entries .
ALSA : echoaudio : check kmalloc () result
sfc : Use kzalloc () to ensure struct efx_spi_device is fully initialised
dmaengine : k3dma : Off by one in k3_of_dma_simple_xlate ()
mfd : max77687 : Add registration of max77686 - clk
[ MTD ] MTD_DEBUG always does compile - time typechecks
xhci : replace USB_MAXINTERFACES with config -> desc . bNumInterface
s390 / cio : add NULL test
ARM : 6891 / 1 : prevent heap corruption in OABI semtimedop
x86 / intel_rdt : Remove redundant ternary operator on return
exofs : use bio_clone_fast in _write_mirror
xfs : fix missing ILOCK unlock when xfs_setattr_nonsize fails due to EDQUOT
usb : return error code when platform_get_irq fails
tracing : Fix off - by - one in trace_get_user ()
x86 / cpu , x86 / pti : Do not enable PTI on AMD processors
inotify : stop kernel memory leak on file creation failure
PCI : Use cpu_hotplug_disable () instead of get_online_cpus ()
drm : fixes when i2c encoder slave mode_fixup is null .
ASoC : wm8753 : remove invalid free of devm_ allocated data
vmwgfx : Unreference surface on cursor error path
mwl8k : prevent freeing free IRQ if ieee80211_register_hw () fails
wl1271 : Fix mac80211 RTS threshold requests during WL1271_STATE_OFF
Staging : otus : hpfwu . c : Checkpatch cleanup
make macfb_setup () static
x86 / ioapic : Prevent NULL pointer dereference in setup_ioapic_dest ()
rxrpc : Validate the net address given to rxrpc_kernel_begin_call ()
crypto : authenc - Use correct ahash complete functions
device property : export irqchip_fwnode_ops
mtd : orion_nand : Fix build failure caused by typo
iucv : Fix missing msg_namelen update in iucv_sock_recvmsg ()
efi / fb : Correct PCI_STD_RESOURCE_END usage
Btrfs : make sure btrfs_remove_free_space doesn ' t leak EAGAIN
[ PATCH ] autofs4 : need to invalidate children on tree mount expire
staging : comedi : das16 : change MODULE_DESCRIPTION
[ PATCH ] epca : update . owner field of struct pci_driver
rtc : m41t80 : add wakealarm functionality
drm / nouveau / disp / nv04 : avoid creation of output paths
Input : sur40 - fix error return code
Driver Core : don ' t oops with unregistered driver in driver_find_device ()
mmc : core : Fix use of uninitialized data in mmc_send_if_cond .
mac80211 : fix rcu locking
iwlwifi : mvm : check if returned value is NULL
drm / i915 : Don ' t try to calculate RC6 residency on GEN4 and before
HID : hid - cypress : validate length of report
[ PATCH ] ieee1394 : fix cross_bound check for null ISO packets
[ media ] s5c73m3 : Fix entity function assignment for the OIF subdev
KEYS : ensure we free the assoc array edit if edit is valid
ipvs : Reject ipv6 link - local addresses for destinations
ALSA : hda / realtek - Fix possible Oops with NULL input_mux
wil6210 : missing reinit_completion in HALP voting
ipv6 : rt6_info output redirect to tunnel output
drm / exynos : fix flags in dma buf exporting
xfs : simple btree query range should look right if LE lookup fails
libata piix3 support warning fix
xen / event : validate irq before get evtchn by irq
KVM : nSVM : always intercept VMLOAD / VMSAVE when nested ( CVE - 2021 - 3656 )
cifs : fix off - by - one bug in build_unc_path_to_root
cfg80211 : fix indentation
usb : message : Fixed parenthesis error in sizeof function .
generic - adc - battery : Fix checking return value of request_any_context_irq
skge : Adjust a null pointer check in skge_down ()
nfsd : clean up reset_union_bmap_deny
drm / tegra : Correct timeout in tegra_syncpt_wait
btrfs : memory leak in btrfs_add_inode_defrag ()
tcp : skb_mstamp_after helper
cpufreq : sfi : Don ' t validate the frequency table twice
Blackfin : debug - mmrs : disable PERIPHERAL_MAP for IMDMA channels
KVM : use after free in kvm_ioctl_create_device ()
drm / i915 : Initialise g4x watermarks for disabled pipes
staging : comedi : das1800 : prefer kmalloc_array over kmalloc with multiply
ucc_geth : Add SUPPORTED_MII and SUPPORTED_Autoneg
clk : st : Fix memory leak
[ SCSI ] megaraid : Use the proper type to hold the irq number .
Bluetooth : Release BTM while sleeping to avoid deadlock
crypto : algif_skcipher - sendmsg SG marking is off by one
ocfs2 : Fix xattr check in ocfs2_get_xattr_nolock ()
fbdev : sm501fb : use memset_io
fix grammar in printk
platform / mellanox : mlxreg - hotplug : Fix uninitialized variable
drm / virtio : Fix NULL vs IS_ERR checking in virtio_gpu_object_shmem_init
staging : ks7010 : change some local variables type in ks_sdio_interrupt
[ PATCH ] pcnet32 : remove incorrect pcnet32_free_ring
fbdev : propagate result of fb_videomode_from_videomode ()
samples / bpf : deal with EBUSY return code from sendmsg in xdpsock sample
drm / i915 : Prevent using uninitialized MMIO funcs
proc : prevent stacking filesystems on top
staging : iio : isl29018 : fix sparse warning regarding incorrect type ( different signedness )
drm : fix uninitialized acquire_ctx fields ( v2 )
ipvs : add check in ftp for initialized core
[ media ] media : platform : s3c - camif : Constify platform_device_id
uprobes : Allocate -> utask before handler_chain () for tracing handlers
hwmon : ( asb100 ) Fix vrm write operation
[ SPARC ]: Add missing of_node_put
mfd : twl6040 : Check for error when reading revision register
embedded : fix vc_translate operator precedence
mm : Fix NULL pointer dereference in madvise ( MADV_WILLNEED ) support
pinctrl : pinconf : fix comparison of different types
USB : option : make interface blacklist work again
iwlwifi : mvm : disable scheduled scan
Staging : ced1401 : Fixes trailing whitespace checkpath warning .
dmapool : fix overflow condition in pool_find_page ()
[ media ] lirc : return ENOTTY when device does support ioctl
i40iw : Fix double free of allocated_buffer
ipvs : allow rescheduling after RST
KVM : x86 : Avoid theoretical NULL pointer dereference in kvm_irq_delivery_to_apic_fast ()
ath9k : Configure AR_PHY_SWITCH_COM with apropriate antenna control
xfrm_user : fix info leak in copy_to_user_auth ()
reiserfs : Don ' t call reiserfs_get_acl () with the reiserfs lock
mfd : Add free max8998 -> ono irq in max8998_irq_exit ()
mac80211 : fix start_seq_num in Rx reorder offload
afs : Add missing afs_put_cell ()
drm / i915 : Hold rcu_read_lock when iterating over the radixtree ( vma idr )
HID : fix error condition propagation in hid - sony driver
iio : st_sensors : make detection more helpful
misc : fastrpc : prevent memory leak in fastrpc_dma_buf_attach
fs / cifs / cifs_dfs_ref . c : fix potential memory leakage
scsi : libiscsi : Fix indentation
tcp : fix potential memory corruption
sched / migration : Move calc_load_migrate () into CPU_DYING
perf intel - pt : Fix off - by - one comparison on maximum code
[ media ] media / vivid - osd : fix info leak in ioctl
partitions / msdos : FreeBSD UFS2 file systems are not recognized
uwb : adds missing error handling
gpio - 74x164 : Fix buffer allocation size
dmaengine : zxdma : fix memset call
drm / radeon / kms : prefer high post dividers in legacy pll algo
assoc_array : don ' t call compare_object () on a node
[ TCP ]: Fix sysctl_tcp_low_latency
[ ALSA ] Add missing compat ioctls for ALSA control API
hwmon : ( w83627hf ) Fix vrm write operation
[ media ] omap3isp : Fix buffer flags handling when querying buffer
[ SERIAL ] 8250_early . c passing 0 instead of NULL
[ media ] videobuf2 : fix sparse warning
ath6kl : Check sme state before delivering disconnect event to cfg80211
staging : lustre : check for integer overflow
[ SPARC64 ]: Allocate TSB node - local .
ACPI : resolve HP nx6125 S3 immediate wakeup regression
virtio : console : Make write () return - ENODEV on hot - unplug
ixgbe : add check for VETO bit when configuring link for KR
irqchip / gic - v3 : Fix out - of - bound access in gic_set_affinity
rtc : limit frequency
[ SCSI ] libfc : fix payload size passed to fc_frame_alloc () in fc_lport_els_request
isdn / gigaset : adjust usb_gigaset tty write buffer limit
iommu / rockchip : Play nice in multi - platform builds
Btrfs : copy string correctly in INO_LOOKUP ioctl
[ CIFS ] Possible null ptr deref in SMB2_tcon
ARM : imx : call imx6sx_cpuidle_init () conditionally for 6sll
drm / exynos : add pid to g2d_runqueue_node
[ media ] drivers / media / pci / saa7134 / saa7134 - dvb . c : Test if videobuf_dvb_get_frontend return NULL
pNFS : Ensure we check layout validity before marking it for return
[ NETFILTER ]: H . 323 helper : fix possible NULL - ptr dereference
hv_netvsc : use kcalloc
lz4 : ensure length does not wrap
ixgb : convert dev -> priv to netdev_priv ( dev ).
chipidea : error on overflow for port_test_write
au1xxx - ide : use ide_find_port ()
drm / nouveau / core : swap the order of imem / fb
igb : Bump version to 3 . 0 . 6
ocfs2 : fix a memory leak in __ocfs2_move_extents ()
drm / mgag200 : check alloc_apertures () success in mga_vram_init ()
phy : exynos - mipi - video : fix check on array index
mmc : atmel - mci : r / w proof capability only available since v2xx
i2c : core : smbus : fix a potential missing - check bug
staging : r8723au : move constant to right of comparison test
drm / nouveau / i2c / gp104 : initial support
ath6kl : unblock fwlog_block_read () on exit
sock_diag : off by one checks
[ SCSI ] scsi_transport_iscsi : make priv_sess file writeable only by root
cdrom : information leak in cdrom_ioctl_media_changed ()
iwlwifi : protect SRAM debugfs
drm / mgag200 : free memory allocated with alloc_apertures ()
mm : enforce min addr even if capable () in expand_downwards ()
inetpeer : initialize -> redirect_genid in inet_getpeer ()
[ ALSA ] hda - codec : Add two new systems to ALC883
esas2r : fix an error path in esas2r_ioctl_handler
cifs : always do is_path_accessible check in cifs_mount
ALSA : seq : Fix race at timer setup and close
drm : restore open_count if drm_setup fails
net_sched : cls_route : remove the right filter from hashtable
mutex : Fix missing conditions to build mutex_spin_on_owner ()
atmel - mci : Fix memory leak in atmci_regs_show
crypto : drbg - fix semicolon . cocci warnings
ovl : ignore permissions on underlying lookup
ax25 : fix info leak via msg_name in ax25_recvmsg ()
prlimit : do_prlimit needs to have a speculation check
block : initialise bd_super in bdget ()
clk : imx7d : Correct ahb clk parent select
bpf : fix liveness propagation to parent in spilled stack slots
ALSA : timer : Fix missing queue indices reset at SNDRV_TIMER_IOCTL_SELECT
drivers : net : xgene : fix possible use after free
reduce B44 _tmpBufferSize ( was allocating two bytes per byte ) (# 843 )
Protected against garbage in the CapRTL encoder
driver : secure : use consttime_memequal for memory comparison
Bug : Lua can generate wrong code when _ENV is < const >
Handle NO response without message properly
dm : validate inputs in vq_endchains
codec : schroedinger : fix potential buffer overflow .
Prevent a CHECK - fail due to empty tensor input in ` map_stage_op . cc `
[ tflite ] Ensure ` MatchingDim ` does not allow buffer overflow .
Avoid an overflow in harfbuzz - myanmar . c