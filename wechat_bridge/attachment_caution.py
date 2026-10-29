"""Bounded filename/MIME signals for the ordinary download caption.

This is a local metadata policy, not file inspection or a malware verdict.
False means only that this finite list found no known signal. Unknown files,
documents and images are never certified safe by this helper. No path is
opened, resolved, executed or inspected, and archives are not unpacked.
"""
import unicodedata


DOWNLOAD_CAUTION = "文件名或声明类型提示可执行、安装包或宏内容，请谨慎打开；本桥未作病毒检测。"

# Finite, local signals. Keep ordinary PDF/images and non-macro Office names
# out of this list; their contents are still outside this metadata check.
CAUTION_EXTENSIONS = frozenset((
    ".exe", ".com", ".dll", ".scr", ".pif", ".cpl", ".hta",
    ".msi", ".msp", ".mst", ".app", ".dmg", ".pkg", ".mpkg",
    ".apk", ".apks", ".deb", ".rpm", ".jar",
    ".bat", ".cmd", ".ps1", ".psm1", ".vbs", ".vbe", ".js", ".jse",
    ".wsf", ".wsh", ".lnk", ".reg", ".sh", ".bash", ".zsh", ".command",
    ".run", ".desktop", ".py", ".pyw", ".so", ".dylib",
    ".docm", ".dotm", ".xlsm", ".xltm", ".xlam", ".xlsb",
    ".pptm", ".potm", ".ppam", ".ppsm", ".sldm",
))
CAUTION_MIMES = frozenset((
    "application/x-executable", "application/x-pie-executable", "application/x-sharedlib",
    "application/vnd.microsoft.portable-executable", "application/x-msdownload",
    "application/x-msdos-program", "application/x-dosexec", "application/x-ms-installer",
    "application/x-msi", "application/x-apple-diskimage", "application/x-apple-installer",
    "application/vnd.apple.installer+xml", "application/vnd.android.package-archive",
    "application/x-debian-package", "application/vnd.debian.binary-package",
    "application/x-rpm", "application/x-redhat-package-manager", "application/java-archive",
    "application/x-java-archive", "application/x-sh", "application/x-csh",
    "application/x-shellscript", "text/x-shellscript", "application/x-bat",
    "application/x-powershell", "text/x-powershell", "application/hta",
    "application/vnd.ms-word.document.macroenabled.12",
    "application/vnd.ms-word.template.macroenabled.12",
    "application/vnd.ms-excel.sheet.macroenabled.12",
    "application/vnd.ms-excel.template.macroenabled.12",
    "application/vnd.ms-excel.addin.macroenabled.12",
    "application/vnd.ms-excel.sheet.binary.macroenabled.12",
    "application/vnd.ms-powerpoint.presentation.macroenabled.12",
    "application/vnd.ms-powerpoint.template.macroenabled.12",
    "application/vnd.ms-powerpoint.slideshow.macroenabled.12",
    "application/vnd.ms-powerpoint.addin.macroenabled.12",
    "application/vnd.ms-powerpoint.slide.macroenabled.12",
))


def _metadata_text(value, maximum):
    if value is None:
        return ""
    if type(value) is not str or len(value) > maximum or "\x00" in value:
        raise ValueError("invalid_attachment_metadata")
    try:
        if len(value.encode("utf-8")) > maximum:
            raise ValueError("invalid_attachment_metadata")
    except UnicodeError:
        raise ValueError("invalid_attachment_metadata") from None
    return value


def needs_download_caution(original_name, declared_type=None):
    """Return whether bounded, already-owned metadata has a known signal."""
    original_name = _metadata_text(original_name, 4096)
    declared_type = _metadata_text(declared_type, 256)
    mime = declared_type.split(";", 1)[0].strip().casefold()
    # Normalization catches common display disguises without reading a file.
    # A filename is display metadata, never a filesystem or shell operand.
    leaf = unicodedata.normalize("NFKC", original_name).replace("\\", "/").rsplit("/", 1)[-1]
    leaf = "".join(char for char in leaf if unicodedata.category(char) != "Cf")
    leaf = leaf.rstrip(" .\t\r\n")
    _, separator, suffix = leaf.rpartition(".")
    return bool(mime in CAUTION_MIMES or separator and ("." + suffix.casefold()) in CAUTION_EXTENSIONS)


def append_download_caution(caption, items):
    """Append the one fixed caution inside the existing physical text budget.

    The public caller enforces its 16000-character model input limit before
    calling. This helper adds at most 128 characters, and does not truncate or
    introduce a new input cap for existing CLI captions. No notification is
    sent here. Only original_name and declared_type are inspected.
    """
    if type(caption) is not str or type(items) not in (list, tuple) or len(items) > 16:
        raise ValueError("invalid_attachment_metadata")
    flagged = False
    for item in items:
        if type(item) is not dict:
            raise ValueError("invalid_attachment_metadata")
        flagged = needs_download_caution(item.get("original_name"), item.get("declared_type")) or flagged
    if not flagged or DOWNLOAD_CAUTION in caption:
        return caption
    separator = "" if not caption or caption.endswith("\n") else "\n"
    return caption + separator + DOWNLOAD_CAUTION
