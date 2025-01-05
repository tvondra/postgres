set terminal svg size 1200,800 fname 'Verdana,10'
set output 'hashjoin.svg'

#set terminal pngcairo size 1600,1000 enhanced font 'Verdana,9'
#set output 'hashjoin.png'

set nokey

set ticslevel 0
set pm3d at b
set xtics offset 0,-0.5 rotate by 45

set xlabel "workmem"
set ylabel "batches"

splot "hash.data" using 1:2:5:xtic(3):ytic(4) with lines lc rgb "#00bb00"
