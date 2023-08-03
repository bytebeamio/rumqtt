import mqtt from "mqtt-packet";
import { printer } from "../printer.js";

const packet = {
    cmd: "suback",
    messageId: 42,
    properties: {
        // MQTT 5.0 properties
        reasonString: "test",
        userProperties: {
            test: "test"
        }
    },
    granted: [0, 1, 2, 128]
} satisfies mqtt.Packet;

const opts = { protocolVersion: 5 } satisfies unknown;

const buffer = mqtt.generate(packet, opts);

printer(buffer);
