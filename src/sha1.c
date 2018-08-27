#define SHA1HANDSOFF
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include "solarisfixes.h"
#include "sha1.h"
#include "config.h"

#define rol(value, bits) (((value) << (bits)) | ((value) >> (32 - (bits))))


