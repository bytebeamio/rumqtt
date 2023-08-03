import mqtt from "mqtt-packet";
import { printer } from "../printer.js";

const packet = {
    cmd: "subscribe",
    messageId: 42,
    subscriptions: [
        {
            topic: "hello/world",
            qos: 1,
            nl: false, // No Local MQTT 5.0 flag
            rap: false, // Retain as Published MQTT 5.0 flag
            rh: 0 // Retain Handling MQTT 5.0
        }
    ]
} satisfies mqtt.Packet;

const opts = { protocolVersion: 5 } satisfies unknown;

const buffer = mqtt.generate(packet, opts);

printer(buffer);
