#!perl6
use v6;
use lib 'lib';
use Net::MQTT;

my $m = Net::MQTT.new(server => 'test.mosquitto.org');
$m.messages.tap: {
    say "{ .<topic> } => { .<message>.decode }";
}

await $m.connection;
