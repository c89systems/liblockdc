#ifndef POUCH_SPACE_CRASH_FIXTURE_H
#define POUCH_SPACE_CRASH_FIXTURE_H

/* These test images start on newly created, zero-filled files. */
static void
crash_fixture_pages(const unsigned char *image,
                    unsigned char pages[PS_DEFAULT_CAPACITY / PS_PAGE]) {
  static const unsigned char zero[PS_PAGE];
  for (size_t page = 0; page < PS_DEFAULT_CAPACITY / PS_PAGE; ++page)
    pages[page] = memcmp(image + page * PS_PAGE, zero, PS_PAGE) != 0;
}

static void crash_fixture_write(int fd, const unsigned char *image,
                                const unsigned char *pages) {
  const size_t count = PS_DEFAULT_CAPACITY / PS_PAGE;
  for (size_t page = 0; page < count;) {
    if (!pages[page]) {
      ++page;
      continue;
    }
    size_t first = page;
    do
      ++page;
    while (page < count && pages[page]);
    size_t offset = first * PS_PAGE, bytes = (page - first) * PS_PAGE;
    REQUIRE(pwrite(fd, image + offset, bytes, (off_t)offset) == (ssize_t)bytes);
  }
}
#endif
