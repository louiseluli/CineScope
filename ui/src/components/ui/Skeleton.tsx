export function LoadingSkeleton({ className = "" }: { className?: string }) {
  return (
    <div
      className={`animate-pulse bg-[var(--surface-hover)] rounded ${className}`}
    />
  );
}

export function CardSkeleton() {
  return (
    <div className="glass-card rounded-xl p-5 border border-[var(--border)]">
      <LoadingSkeleton className="h-4 w-20 mb-2" />
      <LoadingSkeleton className="h-8 w-32 mb-1" />
      <LoadingSkeleton className="h-3 w-24" />
    </div>
  );
}

export function ChartSkeleton({ height = 200 }: { height?: number }) {
  return (
    <div className="glass-card rounded-xl p-6 border border-[var(--border)]">
      <LoadingSkeleton className="h-5 w-40 mb-4" />
      <div className="flex items-end gap-2" style={{ height }}>
        {Array.from({ length: 8 }).map((_, i) => (
          <LoadingSkeleton
            key={i}
            className={`flex-1 ${i % 2 === 0 ? "h-3/4" : "h-1/2"}`}
          />
        ))}
      </div>
    </div>
  );
}

export function PersonCardSkeleton() {
  return (
    <div className="glass-card rounded-xl overflow-hidden border border-[var(--border)]">
      <LoadingSkeleton className="w-full aspect-[2/3]" />
      <div className="p-4">
        <LoadingSkeleton className="h-5 w-3/4 mb-2" />
        <LoadingSkeleton className="h-3 w-1/2" />
      </div>
    </div>
  );
}

export function TableSkeleton({ rows = 5 }: { rows?: number }) {
  return (
    <div className="space-y-2">
      {Array.from({ length: rows }).map((_, i) => (
        <div
          key={i}
          className="flex items-center gap-4 p-3 rounded-lg bg-[var(--surface)]"
        >
          <LoadingSkeleton className="w-10 h-10 rounded-full" />
          <div className="flex-1">
            <LoadingSkeleton className="h-4 w-32 mb-1" />
            <LoadingSkeleton className="h-3 w-24" />
          </div>
          <LoadingSkeleton className="h-6 w-16" />
        </div>
      ))}
    </div>
  );
}

export function FullPageLoader() {
  return (
    <div className="flex items-center justify-center min-h-[400px]">
      <div className="flex flex-col items-center gap-4">
        <div className="relative">
          <div className="w-12 h-12 border-4 border-[var(--border)] rounded-full" />
          <div className="absolute inset-0 w-12 h-12 border-4 border-transparent border-t-[var(--primary)] rounded-full animate-spin" />
        </div>
        <p className="text-[var(--text-muted)] text-sm">Loading...</p>
      </div>
    </div>
  );
}
