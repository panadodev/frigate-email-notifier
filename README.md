# run command

docker run --rm \
 -v "$(pwd)/mosquitto:/mosquitto/config" \
 eclipse-mosquitto:2 \
 mosquitto_passwd -c -b \
 /mosquitto/config/passwd \
 frigateuser 'YOUR_STRONG_PASSWORD'
