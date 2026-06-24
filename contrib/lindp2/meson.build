# Copyright (c) 2022-2026, PostgreSQL Global Development Group

lindp2_sources = files(
  'lindp.c',
)

if host_system == 'windows'
  lindp2_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'lindp2',
    '--FILEDESC', 'lindp - linearized (IKKBZ + LinDP++) join search',])
endif

lindp2 = shared_module('lindp2',
  lindp2_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += lindp2

tests += {
  'name': 'lindp2',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'lindp2',
      'lindp2_fail_lateral',
      'lindp2_fail_nested_outer',
      'lindp2_fail_outer_joins',
      'lindp2_ex1_outer_joins',
      'lindp2_ex2_nested_outer',
      'lindp2_ex3_lateral',
      'lindp2_ex4_right_outer',
      'lindp2_ex5_lateral_outer',
    ],
  },
}
