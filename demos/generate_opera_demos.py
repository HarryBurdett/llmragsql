#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Demo Generator for Opera SE and Opera 3 versions

This script generates Opera-specific demo files from base demo templates.
It applies the following customizations:
1. Opera logos on every page (embedded as base64)
2. Persistent mute that stays for the whole demo
3. No Gemini AI references (replaced with "AI")
4. Working pause button that stops voice narration
5. Proper Opera branding (SE vs Opera 3)

Usage:
    python generate_opera_demos.py

Or import and use:
    from generate_opera_demos import generate_all_demos
    generate_all_demos()
"""

import base64
import re
import os
from pathlib import Path

# Get the directory where this script is located
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent

# Logo paths
OPERA_SE_LOGO_PATH = PROJECT_ROOT / 'frontend' / 'public' / 'opera-se-logo.png'
OPERA3_LOGO_PATH = PROJECT_ROOT / 'frontend' / 'public' / 'opera3-logo.png'


def load_logo_base64(logo_path: Path) -> str:
    """Load a logo file and convert to base64."""
    with open(logo_path, 'rb') as f:
        return base64.b64encode(f.read()).decode('utf-8')


def get_slide_logo_html(base64_data: str, opera_name: str) -> str:
    """Generate HTML for small logo in slide headers."""
    return f'<img src="data:image/png;base64,{base64_data}" alt="{opera_name}" style="height: 32px; width: auto;">'


def get_title_logo_html(base64_data: str, opera_name: str) -> str:
    """Generate HTML for large centered logo on title slides."""
    return f'''<div style="display: flex; align-items: center; justify-content: center; gap: 16px; margin: 20px 0;">
                            <img src="data:image/png;base64,{base64_data}" alt="{opera_name}" style="height: 50px; width: auto;">
                        </div>'''


# Fixed JavaScript functions
FIXED_TOGGLE_AUTOPLAY = '''        function toggleAutoPlay() {
            isPlaying = !isPlaying;
            document.getElementById('playPauseBtn').textContent = isPlaying ? 'Pause' : 'Play';
            if (!isPlaying && 'speechSynthesis' in window) {
                window.speechSynthesis.cancel();
            }
        }'''

FIXED_TOGGLE_VOICE = '''        function toggleVoice() {
            voiceEnabled = !voiceEnabled;
            const icon = document.getElementById('speakerIcon');
            icon.style.opacity = voiceEnabled ? '0.7' : '0.3';
            if (!voiceEnabled) {
                icon.innerHTML = '<path d="M16.5 12c0-1.77-1.02-3.29-2.5-4.03v2.21l2.45 2.45c.03-.2.05-.41.05-.63zm2.5 0c0 .94-.2 1.82-.54 2.64l1.51 1.51C20.63 14.91 21 13.5 21 12c0-4.28-2.99-7.86-7-8.77v2.06c2.89.86 5 3.54 5 6.71zM4.27 3L3 4.27 7.73 9H3v6h4l5 5v-6.73l4.25 4.25c-.67.52-1.42.93-2.25 1.18v2.06c1.38-.31 2.63-.95 3.69-1.81L19.73 21 21 19.73l-9-9L4.27 3zM12 4L9.91 6.09 12 8.18V4z"/>';
            } else {
                icon.innerHTML = '<path d="M3 9v6h4l5 5V4L7 9H3zm13.5 3c0-1.77-1.02-3.29-2.5-4.03v8.05c1.48-.73 2.5-2.25 2.5-4.02zM14 3.23v2.06c2.89.86 5 3.54 5 6.71s-2.11 5.85-5 6.71v2.06c4.01-.91 7-4.49 7-8.77s-2.99-7.86-7-8.77z"/>';
                if (slideNarrations[currentSlide]) {
                    speakSlide(currentSlide);
                }
            }
            if ('speechSynthesis' in window) {
                window.speechSynthesis.cancel();
            }
        }'''

# Original JavaScript functions to replace
ORIGINAL_TOGGLE_AUTOPLAY = '''        function toggleAutoPlay() {
            isPlaying = !isPlaying;
            document.getElementById('playPauseBtn').textContent = isPlaying ? 'Pause' : 'Play';
        }'''

ORIGINAL_TOGGLE_VOICE = '''        function toggleVoice() {
            voiceEnabled = !voiceEnabled;
            const icon = document.getElementById('speakerIcon');
            icon.style.opacity = voiceEnabled ? '0.7' : '0.3';

            if (!voiceEnabled) {
                window.speechSynthesis.cancel();
            } else if (slideNarrations[currentSlide]) {
                speakSlide(currentSlide);
            }
        }'''


def create_opera_demo(source_file: Path, output_file: Path, logo_base64: str, opera_name: str) -> None:
    """
    Create an Opera-specific demo from a base demo template.

    Args:
        source_file: Path to the base demo HTML file
        output_file: Path where the Opera-specific demo will be saved
        logo_base64: Base64-encoded logo image
        opera_name: Display name ("Opera SQL SE" or "Opera 3")
    """
    with open(source_file, 'r') as f:
        content = f.read()

    slide_logo = get_slide_logo_html(logo_base64, opera_name)
    title_logo = get_title_logo_html(logo_base64, opera_name)

    # Determine demo type from filename
    source_name = source_file.name.lower()
    if 'bank-reconciliation' in source_name:
        demo_type = 'Bank Reconciliation'
        content = content.replace(
            '<title>Complete Bank Reconciliation - Crakd.ai Demo</title>',
            f'<title>Bank Reconciliation for {opera_name} - Crakd.ai Demo</title>'
        )
    elif 'gocardless' in source_name:
        demo_type = 'GoCardless Import'
        content = content.replace(
            '<title>GoCardless Import - Crakd.ai Demo</title>',
            f'<title>GoCardless Import for {opera_name} - Crakd.ai Demo</title>'
        )
    elif 'supplier' in source_name:
        demo_type = 'Supplier Statement Automation'
        content = content.replace(
            '<title>Supplier Statement Automation - Crakd.ai Demo</title>',
            f'<title>Supplier Statement Automation for {opera_name} - Crakd.ai Demo</title>'
        )
    else:
        demo_type = 'Demo'

    # Remove Gemini AI references
    content = content.replace("Using Google's Gemini AI, the system", "Using AI, the system")
    content = content.replace("Using Google's Gemini AI, every transaction", "Using AI, every transaction")
    content = content.replace('Powered by Gemini 2.0', 'AI-Powered')
    content = content.replace("Google's Gemini AI", "AI")
    content = content.replace('Gemini AI', 'AI')
    content = content.replace('Gemini 2.0', 'AI')

    # Remove Claude AI references
    content = content.replace("Using Claude's Vision AI, the system", "Using AI, the system")
    content = content.replace("Using Claude's Vision AI, we extract", "Using AI, we extract")
    content = content.replace('Powered by Claude Vision', 'AI-Powered')
    content = content.replace("Claude's Vision AI", "AI")
    content = content.replace("Claude Vision", "AI")
    content = content.replace("Claude AI", "AI")

    # Update narrations to remove Gemini references
    content = content.replace(
        "Using Google's Gemini AI, every transaction is extracted from PDF bank statements regardless of format or bank.",
        "Using AI, every transaction is extracted from PDF bank statements regardless of format or bank."
    )

    # Update header subtitle
    content = content.replace(
        'End-to-End Automated Workflow for Pegasus Opera',
        f'End-to-End Automated Workflow for {opera_name}'
    )
    content = content.replace(
        'Automated Direct Debit Import for Pegasus Opera',
        f'Automated Direct Debit Import for {opera_name}'
    )
    content = content.replace(
        'Intelligent processing, reconciliation & automated communications',
        f'Intelligent processing, reconciliation & automated communications for {opera_name}'
    )

    # Add logo after "AI Automation for" on title slide
    old_pattern = r'(<span style="color: #f59e0b; font-weight: 600;">)Pegasus Opera(</span>\s*</div>\s*</div>)'
    new_replacement = r'\g<1>' + opera_name + r'\2\n                        ' + title_logo
    content = re.sub(old_pattern, new_replacement, content, count=1)

    # Add logo to mock-header elements
    content = content.replace(
        '<span class="mock-title">',
        f'{slide_logo}<span style="margin-left: 8px;"></span><span class="mock-title">'
    )

    # Add logo to slide-content headers (after step-number)
    step_pattern = r'(<div class="step-number">[^<]+</div>\s*<h2>)'
    step_replacement = r'<div style="display: flex; justify-content: space-between; align-items: flex-start;"><div>\1'
    content = re.sub(step_pattern, step_replacement, content)

    content = re.sub(
        r'(<div class="step-number">[^<]+</div>\s*<h2>[^<]+</h2>)',
        r'\1</div>' + slide_logo + '</div>',
        content
    )

    # Update footer references
    content = content.replace(
        'Works seamlessly with <strong style="color: #f59e0b;">Pegasus Opera SQL SE</strong> and <strong style="color: #f59e0b;">Opera 3</strong>',
        f'Seamless integration with <strong style="color: #f59e0b;">{opera_name}</strong>'
    )

    # Update narration references
    content = content.replace(
        'Works with Opera SQL SE and Opera 3',
        f'Seamless integration with {opera_name}'
    )

    # Fix the toggleAutoPlay function (pause button)
    content = content.replace(ORIGINAL_TOGGLE_AUTOPLAY, FIXED_TOGGLE_AUTOPLAY)

    # Fix the toggleVoice function (persistent mute)
    content = content.replace(ORIGINAL_TOGGLE_VOICE, FIXED_TOGGLE_VOICE)

    # Update HTML comment
    content = content.replace(
        '<!-- Demo version: 2026-02-12 - Complete Bank Reconciliation Workflow -->',
        f'<!-- Demo version: 2026-02-12 - {demo_type} for {opera_name} -->'
    )
    content = content.replace(
        '<!-- Demo version: 2026-02-12 - Complete GoCardless Import Workflow -->',
        f'<!-- Demo version: 2026-02-12 - {demo_type} for {opera_name} -->'
    )

    # Add logo to final slide footer
    content = content.replace(
        f'Seamless integration with <strong style="color: #f59e0b;">{opera_name}</strong>',
        f'{slide_logo} Seamless integration with <strong style="color: #f59e0b;">{opera_name}</strong>'
    )

    with open(output_file, 'w') as f:
        f.write(content)

    print(f"Created: {output_file}")


def generate_all_demos(demos_dir: Path = None) -> list:
    """
    Generate all Opera-specific demos from base templates.

    Args:
        demos_dir: Directory containing demo files (defaults to script directory)

    Returns:
        List of paths to generated demo files
    """
    if demos_dir is None:
        demos_dir = SCRIPT_DIR

    # Load logos
    opera_se_base64 = load_logo_base64(OPERA_SE_LOGO_PATH)
    opera3_base64 = load_logo_base64(OPERA3_LOGO_PATH)

    generated_files = []

    # Define base demos and their outputs
    demo_configs = [
        {
            'source': 'bank-reconciliation-complete-demo.html',
            'outputs': [
                ('bank-reconciliation-opera-se-demo.html', opera_se_base64, 'Opera SQL SE'),
                ('bank-reconciliation-opera3-demo.html', opera3_base64, 'Opera 3'),
            ]
        },
        {
            'source': 'gocardless-complete-demo.html',
            'outputs': [
                ('gocardless-opera-se-demo.html', opera_se_base64, 'Opera SQL SE'),
                ('gocardless-opera3-demo.html', opera3_base64, 'Opera 3'),
            ]
        },
        {
            'source': 'supplier-statement-demo.html',
            'outputs': [
                ('supplier-statement-opera-se-demo.html', opera_se_base64, 'Opera SQL SE'),
                ('supplier-statement-opera3-demo.html', opera3_base64, 'Opera 3'),
            ]
        },
    ]

    for config in demo_configs:
        source_path = demos_dir / config['source']
        if not source_path.exists():
            print(f"Warning: Source file not found: {source_path}")
            continue

        for output_name, logo_base64, opera_name in config['outputs']:
            output_path = demos_dir / output_name
            create_opera_demo(source_path, output_path, logo_base64, opera_name)
            generated_files.append(output_path)

    return generated_files


def generate_demo_from_template(template_path: Path, output_dir: Path = None) -> list:
    """
    Generate Opera SE and Opera 3 versions from any demo template.

    Args:
        template_path: Path to the base demo HTML file
        output_dir: Directory for output files (defaults to same as template)

    Returns:
        List of paths to generated demo files
    """
    if output_dir is None:
        output_dir = template_path.parent

    # Load logos
    opera_se_base64 = load_logo_base64(OPERA_SE_LOGO_PATH)
    opera3_base64 = load_logo_base64(OPERA3_LOGO_PATH)

    # Generate output filenames
    base_name = template_path.stem.replace('-complete-demo', '').replace('-demo', '')

    generated_files = []

    for logo_base64, opera_name, suffix in [
        (opera_se_base64, 'Opera SQL SE', 'opera-se'),
        (opera3_base64, 'Opera 3', 'opera3'),
    ]:
        output_path = output_dir / f'{base_name}-{suffix}-demo.html'
        create_opera_demo(template_path, output_path, logo_base64, opera_name)
        generated_files.append(output_path)

    return generated_files


if __name__ == '__main__':
    print("Generating Opera-specific demos...")
    print("=" * 50)

    generated = generate_all_demos()

    print("=" * 50)
    print(f"Generated {len(generated)} demo files:")
    for f in generated:
        print(f"  - {f.name}")
