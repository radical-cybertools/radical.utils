
#include <stdio.h>
#include <sys/time.h>

int main ()
{
    struct timeval tv;
    (void) gettimeofday (&tv, NULL);
    fprintf (stdout, "%ld.%06d\n", tv.tv_sec, tv.tv_usec);
    return (0);
}

