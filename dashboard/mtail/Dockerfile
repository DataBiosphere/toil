FROM jinnlynn/mtail

EXPOSE 3903

COPY ./toil.mtail /home/toil.mtail

CMD ["-progs", "/home/", "-logs", "/dev/stdin"]
