//! Tokenizer module for full-text search
//!
//! Provides multi-language tokenization with:
//! - Unicode NFC normalization
//! - Stop word removal (English, Chinese, Japanese, German, French, Spanish, Italian)
//! - Stemming for Latin-script languages
//! - CJK tokenization via jieba-rs (Chinese) and lindera (Japanese)

use std::collections::HashSet;

use bifrost_hasher::hash_str;
use unicode_normalization::UnicodeNormalization;

// Minimum token length for Latin-script tokens
const MIN_TOKEN_LEN: usize = 2;

// ============================================================================
// Stop Words
// ============================================================================

lazy_static! {
    /// English stop words
    static ref STOP_WORDS_EN: HashSet<&'static str> = {
        [
            "a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
            "has", "he", "in", "is", "it", "its", "of", "on", "that", "the",
            "to", "was", "were", "will", "with", "the", "this", "but", "they",
            "have", "had", "what", "when", "where", "who", "which", "why", "how",
            "all", "each", "every", "both", "few", "more", "most", "other",
            "some", "such", "no", "nor", "not", "only", "own", "same", "so",
            "than", "too", "very", "can", "just", "should", "now", "i", "you",
            "he", "she", "we", "they", "me", "him", "her", "us", "them",
            "my", "your", "his", "her", "our", "their", "mine", "yours",
            "hers", "ours", "theirs", "am", "been", "being", "do", "does",
            "did", "doing", "would", "could", "might", "must", "shall",
            "if", "or", "because", "as", "until", "while", "of", "at", "by",
            "about", "against", "between", "into", "through", "during",
            "before", "after", "above", "below", "up", "down", "out", "off",
            "over", "under", "again", "further", "then", "once", "here",
            "there", "any", "also", "get", "got", "go", "going", "went",
        ].into_iter().collect()
    };

    /// German stop words
    static ref STOP_WORDS_DE: HashSet<&'static str> = {
        [
            "der", "die", "das", "den", "dem", "des", "ein", "eine", "einer",
            "einem", "einen", "und", "oder", "aber", "ist", "sind", "war",
            "waren", "sein", "seine", "seiner", "ihr", "ihre", "ihrer",
            "von", "zu", "mit", "auf", "in", "an", "aus", "bei", "nach",
            "vor", "um", "als", "wenn", "da", "so", "wie", "was", "wer",
            "wo", "wann", "warum", "nicht", "auch", "nur", "noch", "schon",
            "immer", "sehr", "mehr", "viel", "kann", "muss", "soll", "will",
            "ich", "du", "er", "sie", "es", "wir", "ihr", "sie",
        ].into_iter().collect()
    };

    /// French stop words
    static ref STOP_WORDS_FR: HashSet<&'static str> = {
        [
            "le", "la", "les", "un", "une", "des", "du", "de", "et", "ou",
            "mais", "donc", "car", "ni", "est", "sont", "a", "ont", "ai",
            "as", "avons", "avez", "ce", "cette", "ces", "mon", "ma", "mes",
            "ton", "ta", "tes", "son", "sa", "ses", "notre", "votre", "leur",
            "je", "tu", "il", "elle", "nous", "vous", "ils", "elles", "on",
            "qui", "que", "quoi", "dont", "pas", "ne", "plus", "moins",
            "tout", "tous", "toute", "toutes", "avec", "sans", "pour", "par",
            "sur", "sous", "dans", "en", "au", "aux", "ici", "y",
        ].into_iter().collect()
    };

    /// Spanish stop words
    static ref STOP_WORDS_ES: HashSet<&'static str> = {
        [
            "el", "la", "los", "las", "un", "una", "unos", "unas", "y", "o",
            "pero", "porque", "como", "que", "de", "del", "en", "con", "por",
            "para", "sin", "sobre", "entre", "es", "son", "fue", "ser",
            "estar", "ha", "han", "he", "has", "hemos", "yo", "tu", "el",
            "ella", "nosotros", "vosotros", "ellos", "ellas", "mi", "tu",
            "su", "nuestro", "vuestro", "este", "esta", "estos", "estas",
            "ese", "esa", "esos", "esas", "muy", "mas", "menos", "mucho",
            "poco", "todo", "nada", "algo", "alguien", "nadie", "si", "no",
        ].into_iter().collect()
    };

    /// Italian stop words
    static ref STOP_WORDS_IT: HashSet<&'static str> = {
        [
            "il", "lo", "la", "i", "gli", "le", "un", "uno", "una", "e", "o",
            "ma", "che", "di", "da", "in", "con", "su", "per", "tra", "fra",
            "del", "dello", "della", "dei", "degli", "delle", "al", "allo",
            "alla", "ai", "agli", "alle", "dal", "dallo", "dalla", "dai",
            "dagli", "dalle", "nel", "nello", "nella", "nei", "negli", "nelle",
            "io", "tu", "lui", "lei", "noi", "voi", "loro", "mio", "tuo",
            "suo", "nostro", "vostro", "questo", "quello", "sono", "sei",
            "siamo", "siete", "non", "come", "dove", "quando", "perche",
        ].into_iter().collect()
    };

    /// Chinese stop words (common function words and particles)
    static ref STOP_WORDS_ZH: HashSet<&'static str> = {
        [
            "的", "了", "和", "是", "就", "都", "而", "及", "与", "着",
            "或", "一个", "没有", "我们", "你们", "他们", "她们", "它们",
            "这", "那", "这个", "那个", "这些", "那些", "之", "也", "但",
            "不", "在", "有", "为", "以", "对", "等", "可以", "中",
            "到", "被", "从", "把", "让", "给", "用", "比", "很", "更",
            "最", "因为", "所以", "如果", "虽然", "但是", "然后", "只",
            "又", "还", "才", "要", "会", "能", "想", "去", "来", "上",
            "下", "里", "外", "前", "后", "左", "右", "大", "小", "多", "少",
        ].into_iter().collect()
    };

    /// Japanese stop words (particles, auxiliary verbs, common words)
    static ref STOP_WORDS_JA: HashSet<&'static str> = {
        [
            "の", "に", "は", "を", "た", "が", "で", "て", "と", "し",
            "れ", "さ", "ある", "いる", "も", "する", "から", "な", "こと",
            "として", "い", "や", "など", "なっ", "ない", "この", "ため",
            "その", "あっ", "よう", "また", "もの", "という", "あり",
            "まで", "られ", "なる", "へ", "か", "だ", "これ", "によって",
            "により", "おり", "より", "による", "ず", "なり", "られる",
            "において", "ば", "なかっ", "なく", "しかし", "について",
            "せ", "だっ", "その後", "できる", "それ", "う", "ので",
            "なお", "のみ", "でき", "き", "つ", "における", "および",
            "いう", "さらに", "でも", "ら", "たり", "その他", "に関する",
        ].into_iter().collect()
    };

    /// English stemmer (Porter algorithm)
    static ref STEMMER_EN: rust_stemmers::Stemmer = 
        rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::English);

    /// German stemmer
    static ref STEMMER_DE: rust_stemmers::Stemmer = 
        rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::German);

    /// French stemmer
    static ref STEMMER_FR: rust_stemmers::Stemmer = 
        rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::French);

    /// Spanish stemmer
    static ref STEMMER_ES: rust_stemmers::Stemmer = 
        rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Spanish);

    /// Italian stemmer
    static ref STEMMER_IT: rust_stemmers::Stemmer = 
        rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Italian);

    /// Jieba tokenizer for Chinese
    static ref JIEBA: jieba_rs::Jieba = jieba_rs::Jieba::new();

    /// Lindera tokenizer for Japanese
    static ref LINDERA: lindera::Tokenizer = {
        let dictionary = lindera::DictionaryLoader::load_dictionary_from_kind(
            lindera::DictionaryKind::IPADIC
        ).expect("Failed to load IPADIC dictionary");
        lindera::Tokenizer::new(dictionary, None, lindera::Mode::Normal)
    };
}

// ============================================================================
// CJK Detection
// ============================================================================

/// Check if a character is CJK (Chinese, Japanese, Korean)
#[inline]
fn is_cjk_char(c: char) -> bool {
    matches!(c,
        '\u{4E00}'..='\u{9FFF}'     // CJK Unified Ideographs
        | '\u{3400}'..='\u{4DBF}'   // CJK Unified Ideographs Extension A
        | '\u{20000}'..='\u{2A6DF}' // CJK Unified Ideographs Extension B
        | '\u{F900}'..='\u{FAFF}'   // CJK Compatibility Ideographs
        | '\u{2F800}'..='\u{2FA1F}' // CJK Compatibility Ideographs Supplement
    )
}

/// Check if a character is Japanese-specific (Hiragana or Katakana)
#[inline]
fn is_japanese_char(c: char) -> bool {
    matches!(c,
        '\u{3040}'..='\u{309F}'     // Hiragana
        | '\u{30A0}'..='\u{30FF}'   // Katakana
        | '\u{31F0}'..='\u{31FF}'   // Katakana Phonetic Extensions
    )
}

/// Check if a character is Korean Hangul
#[inline]
fn is_korean_char(c: char) -> bool {
    matches!(c,
        '\u{AC00}'..='\u{D7AF}'     // Hangul Syllables
        | '\u{1100}'..='\u{11FF}'   // Hangul Jamo
        | '\u{3130}'..='\u{318F}'   // Hangul Compatibility Jamo
    )
}

/// Detect the dominant script type in a text segment
#[derive(Debug, Clone, Copy, PartialEq)]
enum ScriptType {
    Latin,
    Chinese,
    Japanese,
    Korean,
    Mixed,
}

fn detect_script(text: &str) -> ScriptType {
    let mut cjk_count = 0;
    let mut japanese_count = 0;
    let mut korean_count = 0;
    let mut latin_count = 0;

    for c in text.chars() {
        if is_japanese_char(c) {
            japanese_count += 1;
        } else if is_korean_char(c) {
            korean_count += 1;
        } else if is_cjk_char(c) {
            cjk_count += 1;
        } else if c.is_alphabetic() {
            latin_count += 1;
        }
    }

    // If there's any Japanese-specific characters, treat as Japanese
    if japanese_count > 0 {
        return ScriptType::Japanese;
    }

    // If there's Korean, treat as Korean
    if korean_count > 0 {
        return ScriptType::Korean;
    }

    // If CJK characters dominate, treat as Chinese
    if cjk_count > latin_count {
        return ScriptType::Chinese;
    }

    // Default to Latin
    if latin_count > 0 || (cjk_count == 0 && japanese_count == 0 && korean_count == 0) {
        return ScriptType::Latin;
    }

    ScriptType::Mixed
}

// ============================================================================
// Token Processing
// ============================================================================

/// Check if a token is a stop word (checks all languages)
fn is_stop_word(token: &str) -> bool {
    STOP_WORDS_EN.contains(token)
        || STOP_WORDS_DE.contains(token)
        || STOP_WORDS_FR.contains(token)
        || STOP_WORDS_ES.contains(token)
        || STOP_WORDS_IT.contains(token)
        || STOP_WORDS_ZH.contains(token)
        || STOP_WORDS_JA.contains(token)
}

/// Stem a Latin-script token (tries multiple stemmers, uses shortest result)
fn stem_latin(token: &str) -> String {
    // Use English stemmer as primary (most common)
    let stemmed = STEMMER_EN.stem(token);
    stemmed.into_owned()
}

/// Tokenize Latin-script text
fn tokenize_latin(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();

    for raw in text.split(|c: char| !c.is_alphanumeric()) {
        if raw.len() < MIN_TOKEN_LEN {
            continue;
        }

        let lower = raw.to_lowercase();
        if lower.len() < MIN_TOKEN_LEN {
            continue;
        }

        // Skip stop words
        if is_stop_word(&lower) {
            continue;
        }

        // Apply stemming
        let stemmed = stem_latin(&lower);
        if stemmed.len() >= MIN_TOKEN_LEN {
            tokens.push(stemmed);
        }
    }

    tokens
}

/// Tokenize Chinese text using jieba
fn tokenize_chinese(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();

    for word in JIEBA.cut(text, false) {
        let trimmed = word.trim();
        if trimmed.is_empty() {
            continue;
        }

        // Skip stop words
        if is_stop_word(trimmed) {
            continue;
        }

        // Skip single-character words that are punctuation
        if trimmed.chars().count() == 1 {
            let c = trimmed.chars().next().unwrap();
            if !is_cjk_char(c) && !c.is_alphanumeric() {
                continue;
            }
        }

        tokens.push(trimmed.to_string());
    }

    tokens
}

/// Tokenize Japanese text using lindera
fn tokenize_japanese(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();

    match LINDERA.tokenize(text) {
        Ok(lindera_tokens) => {
            for token in lindera_tokens {
                let surface = token.text;
                if surface.trim().is_empty() {
                    continue;
                }

                // Skip stop words
                if is_stop_word(surface) {
                    continue;
                }

                tokens.push(surface.to_string());
            }
        }
        Err(_) => {
            // Fallback to character-based tokenization
            for c in text.chars() {
                if is_cjk_char(c) || is_japanese_char(c) {
                    tokens.push(c.to_string());
                }
            }
        }
    }

    tokens
}

/// Tokenize Korean text (character n-gram based, since we don't have a Korean tokenizer)
fn tokenize_korean(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();

    // Split on whitespace first
    for word in text.split_whitespace() {
        let trimmed = word.trim();
        if trimmed.is_empty() {
            continue;
        }

        // For Korean, use the whole word as a token
        // Also generate character bigrams for partial matching
        tokens.push(trimmed.to_string());

        // Generate character bigrams
        let chars: Vec<char> = trimmed.chars().collect();
        if chars.len() >= 2 {
            for i in 0..chars.len() - 1 {
                let bigram: String = chars[i..=i + 1].iter().collect();
                tokens.push(bigram);
            }
        }
    }

    tokens
}

// ============================================================================
// Main Tokenization API
// ============================================================================

/// Tokenize text and return hashed tokens
///
/// This is the main entry point for tokenization. It:
/// 1. Applies Unicode NFC normalization
/// 2. Auto-detects the script type
/// 3. Applies appropriate tokenization (Latin, Chinese, Japanese, Korean)
/// 4. Removes stop words
/// 5. Applies stemming for Latin scripts
/// 6. Returns hashed tokens
pub fn tokenize(text: &str) -> Vec<u64> {
    if text.is_empty() {
        return Vec::new();
    }

    // Apply Unicode NFC normalization
    let normalized: String = text.nfc().collect();

    // Detect script and tokenize accordingly
    let script = detect_script(&normalized);

    let tokens = match script {
        ScriptType::Latin => tokenize_latin(&normalized),
        ScriptType::Chinese => tokenize_chinese(&normalized),
        ScriptType::Japanese => tokenize_japanese(&normalized),
        ScriptType::Korean => tokenize_korean(&normalized),
        ScriptType::Mixed => {
            // For mixed content, process segments separately
            tokenize_mixed(&normalized)
        }
    };

    // Hash tokens and deduplicate
    let mut seen = std::collections::HashSet::new();
    let mut result = Vec::new();

    for token in tokens {
        let hash = hash_str(&token);
        if seen.insert(hash) {
            result.push(hash);
        }
    }

    result
}

/// Tokenize text and return tokens with their frequencies
///
/// Similar to `tokenize()` but returns (hash, frequency) pairs
pub fn tokenize_with_freq(text: &str) -> Vec<(u64, u32)> {
    if text.is_empty() {
        return Vec::new();
    }

    // Apply Unicode NFC normalization
    let normalized: String = text.nfc().collect();

    // Detect script and tokenize accordingly
    let script = detect_script(&normalized);

    let tokens = match script {
        ScriptType::Latin => tokenize_latin(&normalized),
        ScriptType::Chinese => tokenize_chinese(&normalized),
        ScriptType::Japanese => tokenize_japanese(&normalized),
        ScriptType::Korean => tokenize_korean(&normalized),
        ScriptType::Mixed => tokenize_mixed(&normalized),
    };

    // Count frequencies
    let mut freq_map = std::collections::HashMap::new();
    for token in tokens {
        let hash = hash_str(&token);
        *freq_map.entry(hash).or_insert(0u32) += 1;
    }

    freq_map.into_iter().collect()
}

/// Tokenize mixed-script content by processing segments separately
fn tokenize_mixed(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current_segment = String::new();
    let mut current_type: Option<ScriptType> = None;

    for c in text.chars() {
        let char_type = if is_japanese_char(c) {
            ScriptType::Japanese
        } else if is_korean_char(c) {
            ScriptType::Korean
        } else if is_cjk_char(c) {
            ScriptType::Chinese
        } else if c.is_alphabetic() || c.is_numeric() {
            ScriptType::Latin
        } else {
            // Whitespace or punctuation - flush current segment
            if !current_segment.is_empty() {
                if let Some(seg_type) = current_type {
                    tokens.extend(tokenize_segment(&current_segment, seg_type));
                }
                current_segment.clear();
                current_type = None;
            }
            continue;
        };

        // If script type changed, flush current segment
        if current_type.is_some() && current_type != Some(char_type) {
            tokens.extend(tokenize_segment(&current_segment, current_type.unwrap()));
            current_segment.clear();
        }

        current_segment.push(c);
        current_type = Some(char_type);
    }

    // Flush remaining segment
    if !current_segment.is_empty() {
        if let Some(seg_type) = current_type {
            tokens.extend(tokenize_segment(&current_segment, seg_type));
        }
    }

    tokens
}

/// Tokenize a segment of known script type
fn tokenize_segment(text: &str, script: ScriptType) -> Vec<String> {
    match script {
        ScriptType::Latin => tokenize_latin(text),
        ScriptType::Chinese => tokenize_chinese(text),
        ScriptType::Japanese => tokenize_japanese(text),
        ScriptType::Korean => tokenize_korean(text),
        ScriptType::Mixed => tokenize_mixed(text),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stop_word_removal_english() {
        let tokens = tokenize("the quick brown fox jumps over the lazy dog");
        // "the", "over" should be filtered out
        assert!(!tokens.is_empty());
        // Should have fewer than 9 tokens due to stop word removal
        assert!(tokens.len() < 9);
    }

    #[test]
    fn test_stemming_english() {
        let tokens1 = tokenize("running");
        let tokens2 = tokenize("runs");
        let tokens3 = tokenize("run");
        // All should produce the same stem
        assert_eq!(tokens1, tokens2);
        assert_eq!(tokens2, tokens3);
    }

    #[test]
    fn test_unicode_normalization() {
        // "café" can be represented as "cafe\u{0301}" (e + combining accent)
        // or as "caf\u{00E9}" (precomposed é)
        let tokens1 = tokenize("café");
        let tokens2 = tokenize("cafe\u{0301}");
        assert_eq!(tokens1, tokens2);
    }

    #[test]
    fn test_chinese_tokenization() {
        let tokens = tokenize("我爱北京天安门");
        assert!(!tokens.is_empty());
        // Jieba should segment this into meaningful words
    }

    #[test]
    fn test_japanese_tokenization() {
        let tokens = tokenize("東京は日本の首都です");
        assert!(!tokens.is_empty());
    }

    #[test]
    fn test_mixed_content() {
        let tokens = tokenize("Hello世界こんにちは");
        assert!(!tokens.is_empty());
        // Should have tokens from English, Chinese, and Japanese
    }

    #[test]
    fn test_empty_input() {
        let tokens = tokenize("");
        assert!(tokens.is_empty());
    }

    #[test]
    fn test_min_token_length() {
        let tokens = tokenize("a b c");
        // Single character tokens should be filtered out for Latin
        assert!(tokens.is_empty());
    }

    #[test]
    fn test_tokenize_with_freq() {
        let freq = tokenize_with_freq("hello hello world");
        assert!(!freq.is_empty());
        // "hello" should have frequency 2 (after stemming if applicable)
    }

    #[test]
    fn test_cjk_detection() {
        assert!(is_cjk_char('中'));
        assert!(is_cjk_char('国'));
        assert!(!is_cjk_char('a'));
        assert!(!is_cjk_char('あ')); // Hiragana is not CJK unified
    }

    #[test]
    fn test_japanese_detection() {
        assert!(is_japanese_char('あ'));
        assert!(is_japanese_char('カ'));
        assert!(!is_japanese_char('中'));
    }

    #[test]
    fn test_script_detection() {
        assert_eq!(detect_script("hello world"), ScriptType::Latin);
        assert_eq!(detect_script("你好世界"), ScriptType::Chinese);
        assert_eq!(detect_script("こんにちは"), ScriptType::Japanese);
        assert_eq!(detect_script("안녕하세요"), ScriptType::Korean);
    }
}
