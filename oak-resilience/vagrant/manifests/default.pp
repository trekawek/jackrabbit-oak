include 'jdk_oracle'

class { '::rabbitmq':
  config_variables => {
    'loopback_users' => "[]"
  }
}
