FROM sscaling/mtail

EXPOSE 3903

COPY ./toil.mtail /home/toil.mtail

ENTRYPOINT ["mtail", "-progs", "/home/", "-logfds", "0"]
