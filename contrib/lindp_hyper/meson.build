# Copyright (c) 2022-2026, PostgreSQL Global Development Group

lindp_hyper_sources = files(
  'lindp.c',
)

if host_system == 'windows'
  lindp_hyper_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'lindp_hyper',
    '--FILEDESC', 'lindp_hyper - LinDP++ join-order search (search-space linearization)',])
endif

lindp_hyper = shared_module('lindp',
  lindp_hyper_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += lindp_hyper

tests += {
  'name': 'lindp_hyper',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'lindp_hyper',
      'lindp_lateral_joins_failure',
      'lindp_outer_joins_failure',
      'lindp_fail_outer_joins',
      'lindp_failure',
      'lindp_fail_full_outer',
      'lindp_fail_lateral',
      'lindp_antijoin',
      'lindp_lateral',
      'lindp_lateral_nested',
    ],
  },
}
