# Copyright (c) 2022-2026, PostgreSQL Global Development Group

lindp3_sources = files(
  'lindp.c',
  'ikkbz.c',
)

if host_system == 'windows'
  lindp3_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'lindp',
    '--FILEDESC', 'lindp - LinDP++ join-order search (search-space linearization)',])
endif

lindp3 = shared_module('lindp',
  lindp3_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += lindp3

tests += {
  'name': 'lindp3',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'lindp3',
      'lindp3_lateral_fail',
      'lindp3_linearization_fail',
    ],
  },
}
