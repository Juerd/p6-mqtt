#!perl6
use v6;
use lib 'lib';
use Net::MQTT;

my $m = Net::MQTT.new(server => 'test.mosquitto.org');

$m.connect.then: {
    $m.publish("hello-world", "$*PID says hi");
    sleep 10;
    $m.publish("hello-world", "$*PID is still here!");
}

$m.messages.tap: {
    say "{ .<topic> } => { .<message>.decode("utf8-c8") }";
}

await $m.connection;
