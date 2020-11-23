const mqtt = require('mqtt-packet');


const packet = {
	cmd: 'subscribe',
	messageId: 42,
	properties: { // MQTT 5.0 properties
		subscriptionIdentifier: 100,
		userProperties: {
			test: 'test'
		}
	},
	subscriptions: [{
		topic: 'hello',
		qos: 1,
		nl: true, // no Local MQTT 5.0 flag
		rap: true, // Retain as Published MQTT 5.0 flag
		rh: 2 // Retain Handling MQTT 5.0
	}]
};


const opts = { protocolVersion: 5 };
let data = mqtt.generate(packet, opts);
print(data);


function print(data) {
	let out = "";
	for (var i = 0; i < data.length; i++) {
		const hex = Number(data[i]).toString(16).padStart(2, '0');
		out = out + "0x" + hex + ", ";
	}

	console.log("[" + out + "]");
}


