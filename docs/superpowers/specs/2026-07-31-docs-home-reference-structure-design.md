# MiniKafka documentation home and reference structure design

## Goal

Give the MiniKafka documentation a real landing page and a consistent page
shape across Quick Start, Architecture, Mapping, Labs, and Differences/Evidence
in both English and Simplified Chinese.

## Information architecture

`docs/index.md` becomes the single bilingual site Home. It introduces the
project, offers clear English and Chinese starting points, summarizes the five
core documentation destinations, and recommends a reading order. Installation
commands and the first experiment move out of Home.

The English quick start moves to `docs/quickstart.md`. The existing
`docs/zh/index.md` remains the Chinese quick start so the already-published
`/zh/` URL does not break. Navigation explicitly lists both quick starts while
mapping `Home / 首页` only to the root landing page.

## Page contract

The five English pages and their five Chinese counterparts follow the same
visible order:

1. one H1 title;
2. a concise purpose statement;
3. the page-specific body;
4. a final Next step / 下一步 section linking to the next destination.

Only the two Quick Start pages retain the compact language switch identifying
English and 中文快速开始. Architecture, Mapping, Labs, and Behavior Matrix
remove their current language banners. Language navigation remains available
through the global MkDocs tabs.

The reading sequence is:

1. Quick Start;
2. Architecture Tour;
3. MiniKafka to Apache Kafka Mapping;
4. Hands-on Labs;
5. Differences and Evidence.

The last page points readers to the chapter tutorial contents rather than
cycling back through the reference sequence.

## Compatibility

The existing Chinese quick-start URL remains valid. Existing English reference
URLs remain unchanged. Links formerly treating root Home as the quick start are
updated to `quickstart.md` only when their intent is explicitly quick-start
navigation.

## Verification

A focused documentation-structure test checks that Home and Quick Start are
separate, both languages expose all five destinations, only Quick Start files
contain the language switch, and every core page starts with H1 and ends with a
Next step heading. Existing bilingual-home coverage remains in place.

The full test suite and strict MkDocs build must pass. Browser acceptance checks
the root Home, both quick starts, the navigation labels, and one long reference
page at desktop and mobile widths.
