import mqtt from "mqtt-packet";
import { printer } from "../printer.js";

const packet = {
    cmd: "subscribe",
    messageId: 42,
    properties: {
        // MQTT 5.0 properties
        subscriptionIdentifier: 100,
        userProperties: {
            test: "test"
        }
    },
    subscriptions: [
        {
            topic: "hello",
            qos: 1,
            nl: true, // No Local MQTT 5.0 flag
            rap: true, // Retain as Published MQTT 5.0 flag
            rh: 2 // Retain Handling MQTT 5.0
        }
    ]
} satisfies mqtt.Packet;

const opts = { protocolVersion: 5 } satisfies unknown;

const buffer = mqtt.generate(packet, opts);

printer(buffer);
