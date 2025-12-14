import { Search, X } from "lucide-react";
import { useState, useEffect, useRef } from "react";
import { clsx } from "clsx";

interface SearchBarProps {
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  className?: string;
  autoFocus?: boolean;
}

export function SearchBar({
  value,
  onChange,
  placeholder = "Search movies, actors, directors...",
  className,
  autoFocus = false,
}: SearchBarProps) {
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (autoFocus && inputRef.current) {
      inputRef.current.focus();
    }
  }, [autoFocus]);

  return (
    <div className={clsx("relative", className)}>
      <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-[var(--text-muted)]" />
      <input
        ref={inputRef}
        type="text"
        value={value}
        onChange={e => onChange(e.target.value)}
        placeholder={placeholder}
        className="w-full pl-10 pr-10 py-3 bg-[var(--surface)] border border-[var(--border)] rounded-lg text-[var(--text)] placeholder:text-[var(--text-muted)] focus:outline-none focus:border-[var(--primary)] transition-colors"
      />
      {value && (
        <button
          onClick={() => onChange("")}
          className="absolute right-3 top-1/2 -translate-y-1/2 text-[var(--text-muted)] hover:text-[var(--text)] transition-colors"
        >
          <X className="w-5 h-5" />
        </button>
      )}
    </div>
  );
}

// Debounced search input
interface DebouncedSearchProps extends SearchBarProps {
  debounceMs?: number;
}

export function DebouncedSearch({
  value: externalValue,
  onChange: onExternalChange,
  debounceMs = 300,
  ...props
}: DebouncedSearchProps) {
  const [localValue, setLocalValue] = useState(externalValue);

  useEffect(() => {
    setLocalValue(externalValue);
  }, [externalValue]);

  useEffect(() => {
    const timer = setTimeout(() => {
      if (localValue !== externalValue) {
        onExternalChange(localValue);
      }
    }, debounceMs);

    return () => clearTimeout(timer);
  }, [localValue, debounceMs, onExternalChange, externalValue]);

  return <SearchBar value={localValue} onChange={setLocalValue} {...props} />;
}
