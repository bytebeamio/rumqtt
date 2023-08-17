import mqtt from "mqtt-packet";
import { printer } from "../printer.js";

/** @type {mqtt.Packet} */
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
};

/** @type {unknown} */
const opts = { protocolVersion: 5 };

const buffer = mqtt.generate(packet, opts);

printer(buffer);
