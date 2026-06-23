# Copyright (c) 2022-2026, PostgreSQL Global Development Group

lindp_sources = files(
  'ikkbz.c',
  'joingraph.c',
  'lindp.c',
)

if host_system == 'windows'
  lindp_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'lindp',
    '--FILEDESC', 'lindp - linearized (IKKBZ + LinDP) join search',])
endif

lindp = shared_module('lindp',
  lindp_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += lindp

tests += {
  'name': 'lindp',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'lindp',
      'lindp_scale',
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
