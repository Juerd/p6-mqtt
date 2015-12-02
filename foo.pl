#!perl6
use v6;
use lib 'lib';
use Net::MQTT;

my $m = Net::MQTT.new(server => 'test.mosquitto.org');

$m.connect.then: {
    $m.subscribe("revspace/#").tap: {
        say "REVSPACE: { .<topic> } => { .<message>.decode("utf8-c8")}";
    }

    $m.subscribe("typing-speed-test.aoeu.eu").tap: {
        say "Typing test completed at { .<message>.decode("utf8-c8") }";
    }

    $m.publish("hello-world", "$*PID says hi");
    sleep 10;
    $m.publish("hello-world", "$*PID is still here!");
}

await $m.connection;
