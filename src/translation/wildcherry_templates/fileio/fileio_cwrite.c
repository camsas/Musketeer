// CamIO 2: fileio_cwrite.c
// Copyright (C) 2013: Matthew P. Grosvenor (matthew.grosvenor@cl.cam.ac.uk) 
// Licensed under BSD 3 Clause, please see LICENSE for more details. 
#include <stdlib.h>
#include <malloc.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/mman.h>


#include "fileio_cwrite.h"
#include "libchaste/include/chaste.h"

#define BUFFSIZE (512 * 1024)




static void wc_fileio_cwrite_open(file_state_t* fs, char* filename)
{
    file_state_cwrite_t* priv = (file_state_cwrite_t*)fs->priv;

    priv->fd = open(filename, O_WRONLY | O_TRUNC | O_CREAT | O_NDELAY | O_NONBLOCK /*| O_DIRECT*/, 0666);
    if(unlikely(priv->fd < 0)){
        ch_log_fatal("Could not open file \"%s\". Error=%s\n", filename, strerror(errno));
    }


    //A ton of cache space that is page alligned
    if(posix_memalign((void*)&priv->buff, getpagesize(), BUFFSIZE) ){
        ch_log_fatal("Could not allocate buffer space - %s\n", strerror(errno));
    }

    priv->size = BUFFSIZE;

}


static ch_byte* wc_fileio_cwrite_read(file_state_t* fs, ch_word* len_out)
{
    ch_log_fatal("Not implemented\n");
    return NULL;
}


static void flush(file_state_t* fs)
{

    file_state_cwrite_t* priv = (file_state_cwrite_t*)fs->priv;

    if(priv->bytes_written > 0 ){
        ch_word written = write(priv->fd, priv->buff, priv->bytes_written);
        if(written < 0){
            ch_log_fatal("Could not write out to file. Error=%s\n", strerror(errno));
        }

        while(written < priv->bytes_written){
            written += write(priv->fd, priv->buff + written , priv->bytes_written - written);
        }

        priv->bytes_written = 0;
    }

}


static ch_word wc_fileio_cwrite_write(file_state_t* fs, ch_byte* data, ch_word len)
{
    file_state_cwrite_t* priv = (file_state_cwrite_t*)fs->priv;

    if(priv->bytes_written + len > priv->size ){
        flush(fs);
    }

    memcpy(priv->buff + priv->bytes_written, data, len);
    priv->bytes_written += len;

    return 0;
}

static void wc_fileio_cwrite_delete(file_state_t* fs)
{
    file_state_cwrite_t* priv = (file_state_cwrite_t*)fs->priv;

    flush(fs);
    close(priv->fd);

    free(priv->buff);
    free(fs);
}


file_state_t* fileio_new_cwrite(char* filename)
{
    file_state_t* result = (file_state_t*)calloc(1,sizeof(file_state_t));
    if(!result){
        ch_log_fatal("Could not allocate memory for new cwrite fileio structure\n");
        return NULL;
    }

    file_state_cwrite_t* priv = (file_state_cwrite_t*)calloc(1, sizeof(file_state_cwrite_t));
    if(!result){
        ch_log_fatal("Could not allocate memory for new cwrite fileio private structure\n");
        return NULL;
    }

    result->priv   = priv;
    result->read   = wc_fileio_cwrite_read;
    result->write  = wc_fileio_cwrite_write;
    result->delete = wc_fileio_cwrite_delete;

    wc_fileio_cwrite_open(result, filename);

    return result;

}
