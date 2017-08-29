
## Latency SYNC
set xlabel "Run On Database"
set ylabel "Total Latency (sec)"
set yrange [0:*]
set rmargin 5
set terminal png
set key left top
set output 'latency_sync.png'

set style data histogram
set style fill solid 1.0 border lt -1
set style histogram errorbars gap 2  lw 2

set datafile separator ","
plot "latency_sync_statictics.csv" using 2:3:4:xtic(1) ti columnhead(2)

clear

## Latency ASYNC
set xlabel "Run On Database"
set ylabel "Total Latency (sec)"
set yrange [0:*]
set rmargin 5
set terminal png
set key left top
set output 'latency_async.png'

set style data histogram
set style fill solid 1.0 border lt -1
set style histogram errorbars gap 2  lw 2

set datafile separator ","
plot "latency_async_statictics.csv" using 2:3:4:xtic(1) ti columnhead(2)

## Latency 
set xlabel "Run On Database"
set ylabel "Total Latency (sec)"
set yrange [0:*]
set rmargin 5
set terminal png
set key left top
set output 'latency.png'

set style data histogram
set style fill solid 1.0 border lt -1
set style histogram errorbars gap 2  lw 2

set datafile separator ","
plot "latency_sync_statictics.csv" using 2:3:4:xtic(1) ti columnhead(2),\
     "latency_async_statictics.csv" using 2:3:4:xtic(1) ti columnhead(2)

############################################################################



## Throughput Sync
set xlabel "Run On Database"
set ylabel "Throughput (Operations/Sec)"
set yrange [0:*]
set rmargin 5
set terminal png
set key left top
set output 'throughput_sync.png'

set style data histogram
set style fill solid 1.0 border lt -1
set style histogram errorbars gap 2  lw 2

set datafile separator ","
plot "throughput_sync_statictics.csv" using 2:3:4:xtic(1) ti columnhead(2)


## Throughput Sync
set xlabel "Run On Database"
set ylabel "Throughput (Operations/Sec)"
set yrange [0:*]
set rmargin 5
set terminal png
set key left top
set output 'throughput_async.png'

set style data histogram
set style fill solid 1.0 border lt -1
set style histogram errorbars gap 2  lw 2

set datafile separator ","
plot "throughput_async_statictics.csv" using 2:3:4:xtic(1) ti columnhead(2)


## Throughput 
set xlabel "Run On Database"
set ylabel "Throughput (Operations/Sec)"
set yrange [0:*]
set rmargin 5
set terminal png
set key left top
set output 'throughput.png'

set style data histogram
set style fill solid 1.0 border lt -1
set style histogram errorbars gap 2  lw 2

set datafile separator ","
plot "throughput_sync_statictics.csv" using 2:3:4:xtic(1) ti columnhead(2),\
      "throughput_async_statictics.csv" using 2:3:4:xtic(1) ti columnhead(2)

