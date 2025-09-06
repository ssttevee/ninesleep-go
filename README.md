# Ninesleep-go

This project started out as a rewrite of https://github.com/bobobo1618/ninesleep in go just cause, but ended up being much more fleshed out than I anticipated.

While the original project was a simple replacement for the "device api client" (or dac) server, I've come up with a way to maintain original app functionality while adding deeper local-only Home Assistant integration (with some [caveats](#Caveats)).

After finishing this project, I found that there is also https://github.com/throwaway31265/free-sleep, which seems to have worked out compatibility with other pod revisions.
Even crazier, https://github.com/LiamSnow/opensleep additionally replaces `frankenfirmware` AND `capybara`.

This is probably as far as I will take this project personally, since I don't plan on buying new pods.
I think this is a good middle-ground between stock and control-freak without being too invasive.
And even if I tried to integrate the improvements from free-sleep, I wouldn't be able to test it.

Feel free to take the web-based [rootfs patcher](jailbreak.html) to lower the bar for people jailbreak their pods. (That might be my only real innovation here lol...)

## Compatibility

Only the Pod 3 has been tested. I only have one bed.

## Instructions

### Disassembly

I found @bobobo1618's instructions to be accurate, but insufficient.
I scoured the internet for videos and pictures from other people shared their experiences.
And even then, I had a very difficult time disassembling my pod.
I hope at least one person finds this more helpful.

It'd be helpful to have some shimming or prying tools.
I used an ifixit kit, but a car trim removal tool kit might also work.

1. (Optional) Unplug the power and USB cables from the back

2. Remove the water reservoir

3. Remove the front fabric mesh

   Pry the corners of the fabric mesh, you might need something longer to reach it (maybe a metal butter knife).
   Stick your pry tool into the gap vertically from top until you hit something (should be just a couple cm).
   Move your pry tool as close to the edge of the fabric as possible while staying as vertical as possible.
   Pull down on your pry tool a little, away from the pod, while trying to pull off the fabric mesh, towards the front.

4. Remove the fan grille on the back

   There are 4 clips on the piece, two on each side.
   Pry the grille from one side, near the top and bottom to unlatch the clips.
   Keep prying on the same side until the whole piece comes out.

5. Remove the two screws on the underside of the hole where the grille was.

   You may need to look up from the floor to see them.
   You do not need to remove the fan screws.

6. Remove the top panel.

   Pry around the top panel to all the clips, near the back half, but away from reservoir hole.
   The front half only glued down and comes apart fairly easily once all the clips are unlatched.
   The back of the cylinder has two massive clips, one on each side, that require a huge effort to unlatch.
   To make things worse, one side has a flexible pcb very close to it, probably a thermal probe for checking water level.
   Be careful not to damage the pcb.

With the top panel off, you can access the pcb and logic board.
The sd card is located under the compute module.
It is glued down from the factory, but you can just scrape it off.
The sd card slot opens by sliding in, away from the edge of the pcb, and flipping open like a book.

### "Jailbreaking"

These are instructions for linux, but mac is almost the same.
I can't be bothered to figure out how to do this on windows.

I also made a [browser tool](https://ssttevee.com/projects/ninesleep-go/jailbreak.html) that automatically handles step 2-10 for you, even on windows.

0. (Optional, but highly recommended) Save an backup image of your sd card in case you screw it up.

Replace `sda` with your disk from `lsblk`. On mac, use `diskutil list` to find the disk.

```bash
sudo dd if=/dev/sda conv=sync bs=1M status=progress | gzip -c > eight-sleep-pod-3-original-firmware.img.gz
```

If you need to restore, run this:

```bash
gunzip --stdout eight-sleep-pod-3-original-firmware.img.gz | sudo dd of=/dev/sda bs=1M status=progress
```

1. Copy `/opt/images/Yocto/rootfs.tar.gz` from the first partition of the sd card.

2. Run these commands in the folder with `rootfs.tar.gz` to set up the workspace

```bash
# decompress the rootfs
gunzip rootfs.tar.gz

# create a workspace for our changes
mkdir -p rootfs

# extract the rootfs to our workspace
tar -xf rootfs.tar -C rootfs
```

3. Download or build (must be `GOOS=linux GOARCH=arm64`) `ninesleep` binary and copy to `rootfs/home/dac/ninesleep`.

4. Copy `ninesleep.service` to `rootfs/etc/systemd/system/ninesleep.service`

If you don't want cloud/app functionality by default, then

5. Enable the service

```bash
ln -s ../ninesleep.service rootfs/etc/systemd/system/multi-user.target.wants/ninesleep.service
```

6. Disable auto updates

```bash
ln -s /dev/null /etc/systemd/system/swupdate.service
```

7. (Optional) Set up the wifi connection

Write this config to `rootfs/etc/NetworkManager/system-connections/customer-wifi.nmconnection`

Make sure to replace `WIFI_NAME` and `WIFI_PASSWORD` with your wifi credentials.

```
[connection]
id=customer-wifi
uuid=632eb932-dd3c-4009-9556-7fa90292b977
type=wifi
interface-name=wlan0
permissions=

[wifi]
mac-address-blacklist=
mode=infrastructure
ssid=WIFI_NAME

[wifi-security]
auth-alg=open
key-mgmt=wpa-psk
psk=WIFI_PASSWORD

[ipv4]
dns-search=
method=auto

[ipv6]
addr-gen-mode=stable-privacy
dns-search=
method=auto

[proxy]
```

Make sure the file permissions is 600.

```bash
chmod 600 rootfs/etc/NetworkManager/system-connections/customer-wifi.nmconnection
```

Thanks @jonatino https://github.com/bobobo1618/ninesleep/pull/5

8. (Optional) Set up local ssh access

Copy your ssh public key to `rootfs/etc/ssh/authorized_keys` and remove the eight sleep's keys.

Set the root password so you can su/sudo

```bash
sudo sed -i "s/root:\\*:/root:$(openssl passwd -1 -salt "$(head -c9 /dev/random | base64)"):/" rootfs/etc/shadow
```

9. Put the new files into the tar with fixed ownership.

```bash
tar --group=0 --owner=0 --numeric-owner -rf rootfs.tar -C rootfs home/dac/ninesleep usr/lib/systemd/system/ninesleep.service etc/systemd/system/multi-user.target.wants/ninesleep.service etc/systemd/system/swupdate.service etc/NetworkManager/system-connections/customer-wifi.nmconnection etc/ssh/authorized_keys etc/shadow
```

10. Recompress `rootfs.tar`

```bash
gzip rootfs.tar
```

11. Reinstall the sd card and compute module back into the pod.

12. Hold the smaller button on the back of the pod, next to the power cable. While the button is held in, plug in the power cable and wait for the front led to flash green before releasing the button.

13. When it stops flashing green, you're done!

If you didn't set up the wifi connection earlier, you'll need to set it up using the eight sleep app.

If it doesn't stop flashing green, something went wrong and you should try restoring the sd card using the backup image.

### Adding to Home Assistant

1. Install the [ESPHome integration](https://www.home-assistant.io/integrations/esphome).

2. Go to Settings > Devices & Services.

Your pod may have been discovered and show up on the list at the top, which you can just click to add.

If not, click the ESPHome integration and click the "ADD DEVICE" button near the top right of the screen.

You'll need to figure out the ip address somehow, either by checking your router for connected devices or just by doing an ip scan with something like [angry ip scanner](https://angryip.org/).

Type in the ip address into the Host box and click submit.

## Caveats

1. The app does not update based on the state of the pod. (Obviously not, why would they?)

In my testing, the app will actually force the pod to match the state of the app.
Meaning, if the pod is turned off in the app and then you turn on the pod from home assistant, after a few minutes, the pod will turn off.
The opposite is also true, if the pod is turned on in the app and then turned off from home assistant, the pod will turn back on after a few minutes.

To work around this, I included a switch in home assistant to toggle the app/cloud connectivity.
This is a completely "safe" operation, meaning you'll never lose access to your pod because of `ninesleep`.
No matter the state of the switch, the pod will reconnect to the cloud if `ninesleep` crashes.

2. The cloud connection queries the device once per second. (approx 15x more than normal)

Whenever the device information is queried, it is also simultaneously sent to home assistant.
While this isn't necessarily a problem, home assistant may more easily become overloaded if you've got many devices with a low power (maybe raspberry pi) home assistant server.

## Home Assistant Entities

All available variables are to Home Assistant as sensors:

- `target_heat_level_left` - same as app temperature multiplied by 10, not real temperature (ranges from -100 to 100)
- `target_heat_level_right` - same as app temperature multiplied by 10, not real temperature (ranges from -100 to 100)
- `heat_time_left_seconds` - seconds until the left side of the pod turns off
- `heat_time_right_seconds` - seconds until the right side of the pod turns off
- `heat_level_left` - the current "temperature" of the left side of the pod
- `heat_level_right` - the current "temperature" of the right side of the pod
- `led_brightness` - led brightness level
- `water_level_ok` - water level ok
- `priming_active` - priming active
- `sensor_label` - ? (disabled by default)
- `settings_raw` - cbor encoded settings (disabled by default)

There are also some basic controls:

- `target_heat_level_left` - temperature number left side
- `target_heat_level_right` - temperature number right side
- `led_brightness` - led brightness level
- `heat_left` - toggles pod left side
- `heat_right` - toggles pod right side
- `prime` - starts pod priming (no way to stop it without unpluggin the pod)
- `firmware_reset` - restarts the pod firmware, but doesn't reboot the os (disabled by default)
- `power_reset` - reboots the pod os (disabled by default)
- `factory_reset` - same as holding the small button on the back while plugging in the power (disabled by default)

There are some "meta" entities about `ninesleep` itself:

- `pod_heartbeat_epoch` - unix timestamp of the last update from the pod firmware (disabled by default)
- `pod_available` - true when connected to the pod firmware
- `cloud_connected` - true when connected to the cloud/app
- `enable_cloud` - switch to turn on and off cloud/app connectivity

And finally there are some higher level abstraction entities:

- `sync_sides` - syncs the left and right sides of the pod
- `climate_left` - simulates a thermostat with a "real" temperature value in celcius
- `climate_right` - simulates a thermostat with a "real" temperature value in celcius
- `climate_temperature_offset` - offset value for the "real" temperature, doesn't really work (disabled by default)

## Development Notes

- The pod's default ssh port is `8822`
- The dac is written in js, you can pull the code from the sd card from `/home/dac/app`
- gosthome is somewhat incomplete, many of the entity actions aren't hooked up
- I'm almost certain that there are more settings for controlling the LED, but mine seemed to stop changing colours...

### Maintaining app functionality/graceful failure

The `dac` service binds to `/deviceinfo/dac.sock` and wait for `frankenfirmware` to connect (seems to attempt every 30 seconds or so), for controlling the device.
I assume that it was designed like this to guarantee only a single process connected at any time, but you can just not `accept()` so I don't really know.

When `ninesleep` wants to connect to `frankenfirmware` (i.e. on launch and when it looses connection homehow?), it temporary stops the `dac` service using `systemctl`.
Then it binds to `/deviceinfo/dac.sock`, pretending to be `dac`.
When `frankenfirmware` is connected, unbind from `/deviceinfo/dac.sock`.
This does not kill file descriptor, so the connection remains alive.
Then restart `dac` and let it bind to `/deviceinfo/dac.sock` like normal.
Nothing will happen at this point because `frankenfirmware` is still connected.
If `ninesleep` crashes, `frankenfirmware` will try to reconnect to `dac` like normal.

The other part of this is that `ninesleep` can also pretend to be `frankenfirmware` and connect to `/deviceinfo/dac.sock`, then proxy the requests.
This is otherwise known as a mitm, or man in the middle, proxy.
In this mode, the app will be none the wiser and work as nothing is different.
