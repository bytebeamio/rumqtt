

### Build and run
------------------

```
./build.sh
docker run -it -p 1883:1883 --name rumqttd rumqttd			# run foreground
docker run -d -p 1883:1883 --name rumqttd rumqttd			# run background
docker run -d -p 1883:1883 -e "RUST_LOG=debug" --name rumqttd rumqttd   # override loglevel
```
