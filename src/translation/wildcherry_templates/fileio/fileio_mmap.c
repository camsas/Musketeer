// CamIO 2: fileio_mmap.c
// Copyright (C) 2013: Matthew P. Grosvenor (matthew.grosvenor@cl.cam.ac.uk) 
// Licensed under BSD 3 Clause, please see LICENSE for more details. 
#include <stdlib.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/mman.h>

#include "fileio_mmap.h"
#include "libchaste/include/chaste.h"

static void wc_fileio_mmap_open(file_state_t* fs, char* filename)
{
    file_state_mmap_t* priv = (file_state_mmap_t*)fs->priv;

    priv->fd = open(filename, O_RDONLY);
    if(unlikely(priv->fd < 0)){
        ch_log_fatal("Could not open file \"%s\". Error=%s\n", filename, strerror(errno));
    }

    //Get the file size
    struct stat st;
    fstat(priv->fd, &st);
    priv->size = st.st_size;

    if(priv->size)
    {
        //Map the whole thing into memory
        priv->buff = (ch_byte*)mmap( NULL, priv->size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, priv->fd, 0);
        if(unlikely(priv->buff == MAP_FAILED)){
            ch_log_fatal("Could not memory map file \"%s\". Error=%s\n", filename, strerror(errno));
        }
    }
}


static ch_byte* wc_fileio_mmap_read(file_state_t* fs, ch_word* len_out)
{
    file_state_mmap_t* priv = (file_state_mmap_t*)fs->priv;
    if(priv->bytes_read < priv->size){
        *len_out = priv->size;
        priv->bytes_read += priv->size;
        return priv->buff;
    }

    *len_out = 0;
    return NULL;
}

static void wc_fileio_mmap_delete(file_state_t* fs)
{
    file_state_mmap_t* priv = (file_state_mmap_t*)fs->priv;

    munmap(priv->buff, priv->size);

    close(priv->fd);

    free(fs);
}


static ch_word wc_fileio_mmap_write(file_state_t* fs, ch_byte* data, ch_word len)
{
    ch_log_fatal("Not implemented\n");
    return 0;
}


file_state_t* fileio_new_mmap(char* filename)
{
    file_state_t* result = (file_state_t*)calloc(1,sizeof(file_state_t));
    if(!result){
        ch_log_fatal("Could not allocate memory for new mmap fileio structure\n");
        return NULL;
    }

    file_state_mmap_t* priv = (file_state_mmap_t*)calloc(1, sizeof(file_state_mmap_t));
    if(!result){
        ch_log_fatal("Could not allocate memory for new mmap fileio private structure\n");
        return NULL;
    }

    result->priv     = priv;
    result->read   = wc_fileio_mmap_read;
    result->write  = wc_fileio_mmap_write;
    result->delete = wc_fileio_mmap_delete;

    wc_fileio_mmap_open(result, filename);

    return result;

}
