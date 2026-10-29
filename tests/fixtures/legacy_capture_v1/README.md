# Offline legacy capture fixture

These two files are unchanged legacy source bytes used only by
`FailureCaptureTests.test_real_legacy_bootstrap_zero_and_half_write_can_crash_again`.
They are not installed runtime modules. The test validates both complete files
before publishing a temporary isolated package; no Git history or network is
needed. A missing or changed fixture fails the test instead of skipping it.

Source provenance identifier: `adeaa801bbc30e52254ed9dfe9b3fa3a16344398`.
This identifier is documentation, not a required object in a public checkout.
No ancestor commits or Git metadata accompany this fixture.

| Original path | Bytes | Original Git blob SHA-1 | File SHA-256 |
| --- | ---: | --- | --- |
| `wechat_bridge/failure_capture.py` | 17640 | `533dd817b3a3304b477d37bedaedc6238480bba6` | `9f05aacf7b16d4231cb8a53b0910250bd728dbb6448057a63a8bed39305c751b` |
| `wechat_bridge/media_diagnostics.py` | 2450 | `550220af9d39eac56cded2c8a09f6db0469fad56` | `138a24e589af9891341e087c692f3d3d036d148d81ad6ca138b1325e148968e6` |

The original MIT notice is retained in [LICENSE](LICENSE), identical to the
repository license. The fixture's only package-local dependency is the second
file; its remaining imports are Python standard-library modules. Its POSIX
filesystem requirements still apply. Removing a Git-history dependency does not
make this regression a Windows compatibility test.
