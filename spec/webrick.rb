
require 'webrick'

params = {
    :DocumentRoot => './coverage',
    :BindAddress => '0.0.0.0',
    :Port => 8080
}
puts "Please Access #{params[:BindAddress]}:#{params[:Port]}"
srv = WEBrick::HTTPServer.new(params)

trap(:INT){ srv.shutdown }
srv.start
