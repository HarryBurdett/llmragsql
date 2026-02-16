import React, { useState, useRef, useEffect } from 'react';

interface Option {
  value: string;
  label: string;
  sublabel?: string;
}

interface SearchableSelectProps {
  options: Option[];
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  className?: string;
  disabled?: boolean;
  displayValue?: string; // What to show when selected (if different from finding in options)
}

export function SearchableSelect({
  options,
  value,
  onChange,
  placeholder = 'Search...',
  className = '',
  disabled = false,
  displayValue
}: SearchableSelectProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [searchText, setSearchText] = useState('');
  const inputRef = useRef<HTMLInputElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  // Get display text for current value
  const getDisplayText = () => {
    if (displayValue) return displayValue;
    if (!value) return '';
    const option = options.find(o => o.value === value);
    return option ? option.label : value;
  };

  // Filter options based on search
  const filteredOptions = options.filter(option => {
    if (!searchText) return true;
    const search = searchText.toLowerCase();
    return option.label.toLowerCase().includes(search) ||
           option.value.toLowerCase().includes(search) ||
           (option.sublabel && option.sublabel.toLowerCase().includes(search));
  });

  // Handle click outside to close
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(event.target as Node)) {
        setIsOpen(false);
        setSearchText('');
      }
    };

    if (isOpen) {
      document.addEventListener('mousedown', handleClickOutside);
    }
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, [isOpen]);

  // Handle keyboard navigation
  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Escape') {
      setIsOpen(false);
      setSearchText('');
    } else if (e.key === 'Enter' && filteredOptions.length > 0) {
      onChange(filteredOptions[0].value);
      setIsOpen(false);
      setSearchText('');
    }
  };

  return (
    <div ref={containerRef} className={`relative ${className}`}>
      <input
        ref={inputRef}
        type="text"
        value={isOpen ? searchText : getDisplayText()}
        onChange={(e) => {
          setSearchText(e.target.value);
          if (!isOpen) setIsOpen(true);
        }}
        onFocus={() => {
          setIsOpen(true);
          setSearchText('');
        }}
        onKeyDown={handleKeyDown}
        placeholder={placeholder}
        disabled={disabled}
        className={`w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500 ${
          disabled ? 'bg-gray-100 cursor-not-allowed' : ''
        }`}
      />

      {isOpen && !disabled && (
        <div className="absolute z-50 w-full mt-1 bg-white border border-gray-300 rounded-md shadow-lg max-h-60 overflow-y-auto">
          {filteredOptions.slice(0, 100).map(option => (
            <button
              key={option.value}
              type="button"
              onClick={() => {
                onChange(option.value);
                setIsOpen(false);
                setSearchText('');
              }}
              className={`w-full text-left px-3 py-2 hover:bg-blue-50 text-sm ${
                value === option.value ? 'bg-blue-100 text-blue-800' : ''
              }`}
            >
              <span className="font-medium">{option.label}</span>
              {option.sublabel && (
                <span className="text-gray-500 text-xs block">{option.sublabel}</span>
              )}
            </button>
          ))}
          {filteredOptions.length === 0 && (
            <div className="px-3 py-2 text-sm text-gray-500">No matches found</div>
          )}
          {filteredOptions.length > 100 && (
            <div className="px-3 py-2 text-sm text-gray-400 italic">
              Showing first 100 results. Type more to narrow down.
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// Compact version for table cells
export function SearchableSelectCompact({
  options,
  value,
  onChange,
  placeholder = 'Search...',
  className = '',
  disabled = false,
  displayValue
}: SearchableSelectProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [searchText, setSearchText] = useState('');
  const containerRef = useRef<HTMLDivElement>(null);

  const getDisplayText = () => {
    if (displayValue) return displayValue;
    if (!value) return '';
    const option = options.find(o => o.value === value);
    return option ? option.label : value;
  };

  const filteredOptions = options.filter(option => {
    if (!searchText) return true;
    const search = searchText.toLowerCase();
    return option.label.toLowerCase().includes(search) ||
           option.value.toLowerCase().includes(search) ||
           (option.sublabel && option.sublabel.toLowerCase().includes(search));
  });

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(event.target as Node)) {
        setIsOpen(false);
        setSearchText('');
      }
    };

    if (isOpen) {
      document.addEventListener('mousedown', handleClickOutside);
    }
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, [isOpen]);

  return (
    <div ref={containerRef} className={`relative ${className}`}>
      <input
        type="text"
        value={isOpen ? searchText : getDisplayText()}
        onChange={(e) => {
          setSearchText(e.target.value);
          if (!isOpen) setIsOpen(true);
        }}
        onFocus={() => {
          setIsOpen(true);
          setSearchText('');
        }}
        placeholder={placeholder}
        disabled={disabled}
        className={`w-full text-sm px-2 py-1 border rounded ${
          value ? 'border-green-400 bg-green-50' : 'border-gray-300'
        } ${disabled ? 'bg-gray-100 cursor-not-allowed' : ''}`}
      />

      {isOpen && !disabled && (
        <div className="absolute z-50 w-64 mt-1 bg-white border border-gray-300 rounded-md shadow-lg max-h-48 overflow-y-auto">
          {filteredOptions.slice(0, 50).map(option => (
            <button
              key={option.value}
              type="button"
              onClick={() => {
                onChange(option.value);
                setIsOpen(false);
                setSearchText('');
              }}
              className={`w-full text-left px-2 py-1.5 hover:bg-blue-50 text-sm ${
                value === option.value ? 'bg-blue-100 text-blue-800' : ''
              }`}
            >
              <span className="font-medium">{option.label}</span>
              {option.sublabel && (
                <span className="text-gray-500 text-xs block">{option.sublabel}</span>
              )}
            </button>
          ))}
          {filteredOptions.length === 0 && (
            <div className="px-2 py-1.5 text-sm text-gray-500">No matches found</div>
          )}
        </div>
      )}
    </div>
  );
}
