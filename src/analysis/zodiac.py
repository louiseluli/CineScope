"""
CineScope Zodiac & Astrology Calculator

Calculates Western zodiac signs, Chinese zodiac, and related astrological data
from birth dates. A fun analytical layer for people data.

Usage:
    from src.analysis.zodiac import ZodiacCalculator
    
    calc = ZodiacCalculator()
    result = calc.calculate("1974-11-11")
    # {'western_sign': 'Scorpio', 'western_symbol': '♏', ...}
"""
from datetime import datetime
from typing import Dict, Optional, Tuple


# Western Zodiac Sign Definitions
# (month, day) = last day of that sign
WESTERN_ZODIAC = [
    ((1, 19), 'Capricorn', '♑', 'Earth', 'Cardinal', 'Saturn'),
    ((2, 18), 'Aquarius', '♒', 'Air', 'Fixed', 'Uranus'),
    ((3, 20), 'Pisces', '♓', 'Water', 'Mutable', 'Neptune'),
    ((4, 19), 'Aries', '♈', 'Fire', 'Cardinal', 'Mars'),
    ((5, 20), 'Taurus', '♉', 'Earth', 'Fixed', 'Venus'),
    ((6, 20), 'Gemini', '♊', 'Air', 'Mutable', 'Mercury'),
    ((7, 22), 'Cancer', '♋', 'Water', 'Cardinal', 'Moon'),
    ((8, 22), 'Leo', '♌', 'Fire', 'Fixed', 'Sun'),
    ((9, 22), 'Virgo', '♍', 'Earth', 'Mutable', 'Mercury'),
    ((10, 22), 'Libra', '♎', 'Air', 'Cardinal', 'Venus'),
    ((11, 21), 'Scorpio', '♏', 'Water', 'Fixed', 'Pluto'),
    ((12, 21), 'Sagittarius', '♐', 'Fire', 'Mutable', 'Jupiter'),
    ((12, 31), 'Capricorn', '♑', 'Earth', 'Cardinal', 'Saturn'),
]

# Chinese Zodiac (based on year % 12)
CHINESE_ZODIAC = {
    0: ('Monkey', '🐵', 'Metal', 'Yang'),
    1: ('Rooster', '🐔', 'Metal', 'Yin'),
    2: ('Dog', '🐕', 'Earth', 'Yang'),
    3: ('Pig', '🐷', 'Water', 'Yin'),
    4: ('Rat', '🐀', 'Water', 'Yang'),
    5: ('Ox', '🐂', 'Earth', 'Yin'),
    6: ('Tiger', '🐅', 'Wood', 'Yang'),
    7: ('Rabbit', '🐇', 'Wood', 'Yin'),
    8: ('Dragon', '🐉', 'Earth', 'Yang'),
    9: ('Snake', '🐍', 'Fire', 'Yin'),
    10: ('Horse', '🐴', 'Fire', 'Yang'),
    11: ('Goat', '🐐', 'Earth', 'Yin'),
}

# Sign personality traits (for fun analytics)
SIGN_TRAITS = {
    'Aries': ['bold', 'ambitious', 'impulsive', 'competitive'],
    'Taurus': ['reliable', 'patient', 'stubborn', 'sensual'],
    'Gemini': ['adaptable', 'curious', 'restless', 'communicative'],
    'Cancer': ['nurturing', 'emotional', 'protective', 'intuitive'],
    'Leo': ['confident', 'dramatic', 'generous', 'proud'],
    'Virgo': ['analytical', 'practical', 'perfectionist', 'helpful'],
    'Libra': ['diplomatic', 'harmonious', 'indecisive', 'social'],
    'Scorpio': ['intense', 'passionate', 'secretive', 'transformative'],
    'Sagittarius': ['adventurous', 'optimistic', 'restless', 'philosophical'],
    'Capricorn': ['ambitious', 'disciplined', 'reserved', 'patient'],
    'Aquarius': ['innovative', 'independent', 'humanitarian', 'rebellious'],
    'Pisces': ['intuitive', 'artistic', 'escapist', 'compassionate'],
}


class ZodiacCalculator:
    """Calculate zodiac signs and astrological data from birth dates."""
    
    def calculate(self, birth_date: str) -> Optional[Dict]:
        """
        Calculate all zodiac information from a birth date.
        
        Args:
            birth_date: Date string in format 'YYYY-MM-DD' or 'YYYY'
            
        Returns:
            Dictionary with zodiac information, or None if date invalid
        """
        parsed = self._parse_date(birth_date)
        if not parsed:
            return None
            
        year, month, day = parsed
        
        result = {
            'birth_year': year,
            'birth_month': month,
            'birth_day': day,
        }
        
        # Western zodiac (needs full date)
        if month and day:
            western = self._get_western_zodiac(month, day)
            result.update({
                'western_sign': western['sign'],
                'western_symbol': western['symbol'],
                'western_element': western['element'],
                'western_modality': western['modality'],
                'western_ruling_planet': western['ruling_planet'],
                'western_traits': SIGN_TRAITS.get(western['sign'], []),
            })
        
        # Chinese zodiac (needs year)
        if year:
            chinese = self._get_chinese_zodiac(year)
            result.update({
                'chinese_animal': chinese['animal'],
                'chinese_symbol': chinese['symbol'],
                'chinese_element': chinese['element'],
                'chinese_polarity': chinese['polarity'],
            })
        
        # Birthstone and birth flower (month-based)
        if month:
            result['birthstone'] = self._get_birthstone(month)
            result['birth_flower'] = self._get_birth_flower(month)
        
        return result
    
    def _parse_date(self, date_str: str) -> Optional[Tuple[int, Optional[int], Optional[int]]]:
        """Parse date string, handling various formats."""
        if not date_str:
            return None
            
        date_str = str(date_str).strip()
        
        # Try full date formats
        for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%d-%m-%Y', '%d/%m/%Y']:
            try:
                dt = datetime.strptime(date_str, fmt)
                return (dt.year, dt.month, dt.day)
            except ValueError:
                continue
        
        # Try year only
        if date_str.isdigit() and len(date_str) == 4:
            return (int(date_str), None, None)
        
        # Try year from longer string (e.g., "1974-01-01" approximations)
        if len(date_str) >= 4 and date_str[:4].isdigit():
            year = int(date_str[:4])
            if 1800 <= year <= 2100:
                # Try to extract month and day
                if len(date_str) >= 10:
                    try:
                        dt = datetime.strptime(date_str[:10], '%Y-%m-%d')
                        return (dt.year, dt.month, dt.day)
                    except ValueError:
                        pass
                return (year, None, None)
        
        return None
    
    def _get_western_zodiac(self, month: int, day: int) -> Dict:
        """Determine Western zodiac sign from month and day."""
        for (end_month, end_day), sign, symbol, element, modality, ruler in WESTERN_ZODIAC:
            if month < end_month or (month == end_month and day <= end_day):
                return {
                    'sign': sign,
                    'symbol': symbol,
                    'element': element,
                    'modality': modality,
                    'ruling_planet': ruler,
                }
        
        # Default to Capricorn (shouldn't reach here)
        return {
            'sign': 'Capricorn',
            'symbol': '♑',
            'element': 'Earth',
            'modality': 'Cardinal',
            'ruling_planet': 'Saturn',
        }
    
    def _get_chinese_zodiac(self, year: int) -> Dict:
        """Determine Chinese zodiac from year."""
        # Simple calculation (doesn't account for lunar new year)
        index = year % 12
        animal, symbol, element, polarity = CHINESE_ZODIAC[index]
        
        return {
            'animal': animal,
            'symbol': symbol,
            'element': element,
            'polarity': polarity,
        }
    
    def _get_birthstone(self, month: int) -> str:
        """Get birthstone for month."""
        birthstones = {
            1: 'Garnet', 2: 'Amethyst', 3: 'Aquamarine',
            4: 'Diamond', 5: 'Emerald', 6: 'Pearl',
            7: 'Ruby', 8: 'Peridot', 9: 'Sapphire',
            10: 'Opal', 11: 'Topaz', 12: 'Turquoise'
        }
        return birthstones.get(month, 'Unknown')
    
    def _get_birth_flower(self, month: int) -> str:
        """Get birth flower for month."""
        flowers = {
            1: 'Carnation', 2: 'Violet', 3: 'Daffodil',
            4: 'Daisy', 5: 'Lily of the Valley', 6: 'Rose',
            7: 'Larkspur', 8: 'Gladiolus', 9: 'Aster',
            10: 'Marigold', 11: 'Chrysanthemum', 12: 'Poinsettia'
        }
        return flowers.get(month, 'Unknown')
    
    def get_sign_compatibility(self, sign1: str, sign2: str) -> Dict:
        """
        Calculate compatibility between two zodiac signs.
        Returns a fun compatibility analysis.
        """
        # Element compatibility
        element_compat = {
            ('Fire', 'Fire'): 0.8, ('Fire', 'Air'): 0.9, ('Fire', 'Earth'): 0.5, ('Fire', 'Water'): 0.4,
            ('Air', 'Air'): 0.7, ('Air', 'Earth'): 0.5, ('Air', 'Water'): 0.6,
            ('Earth', 'Earth'): 0.8, ('Earth', 'Water'): 0.9,
            ('Water', 'Water'): 0.7,
        }
        
        elements = {}
        for _, sign, _, element, _, _ in WESTERN_ZODIAC:
            elements[sign] = element
        
        e1, e2 = elements.get(sign1), elements.get(sign2)
        if not e1 or not e2:
            return {'score': 0.5, 'description': 'Unknown compatibility'}
        
        # Make key order-independent
        key = tuple(sorted([e1, e2]))
        score = element_compat.get(key, 0.5)
        
        descriptions = {
            (0.9, 1.0): 'Excellent match! Natural harmony.',
            (0.8, 0.9): 'Great compatibility. Strong connection.',
            (0.7, 0.8): 'Good match. Understanding comes easily.',
            (0.5, 0.7): 'Moderate compatibility. Some effort needed.',
            (0.0, 0.5): 'Challenging pairing. Requires patience.',
        }
        
        desc = 'Moderate compatibility'
        for (low, high), d in descriptions.items():
            if low <= score < high:
                desc = d
                break
        
        return {
            'score': score,
            'percentage': int(score * 100),
            'description': desc,
            'elements': f'{e1} + {e2}',
        }


def calculate_age(birth_date: str, death_date: str = None, reference_date: str = None) -> Optional[int]:
    """
    Calculate age from birth date.
    
    Args:
        birth_date: Birth date string
        death_date: Death date string (if deceased)
        reference_date: Date to calculate age at (defaults to today or death_date)
        
    Returns:
        Age in years, or None if cannot calculate
    """
    calc = ZodiacCalculator()
    
    birth = calc._parse_date(birth_date)
    if not birth or not birth[0]:
        return None
    
    birth_year = birth[0]
    birth_month = birth[1] or 1
    birth_day = birth[2] or 1
    
    # Determine end date
    if death_date:
        death = calc._parse_date(death_date)
        if death and death[0]:
            end_year = death[0]
            end_month = death[1] or 12
            end_day = death[2] or 31
        else:
            return None
    elif reference_date:
        ref = calc._parse_date(reference_date)
        if ref and ref[0]:
            end_year, end_month, end_day = ref[0], ref[1] or 1, ref[2] or 1
        else:
            return None
    else:
        today = datetime.now()
        end_year, end_month, end_day = today.year, today.month, today.day
    
    # Calculate age
    age = end_year - birth_year
    
    # Adjust if birthday hasn't occurred yet
    if end_month and birth_month:
        if (end_month, end_day) < (birth_month, birth_day):
            age -= 1
    
    return max(0, age)


# Convenience functions
def get_zodiac_sign(birth_date: str) -> Optional[str]:
    """Get just the Western zodiac sign name."""
    calc = ZodiacCalculator()
    result = calc.calculate(birth_date)
    return result.get('western_sign') if result else None


def get_zodiac_symbol(birth_date: str) -> Optional[str]:
    """Get just the Western zodiac symbol."""
    calc = ZodiacCalculator()
    result = calc.calculate(birth_date)
    return result.get('western_symbol') if result else None


def get_chinese_animal(birth_date: str) -> Optional[str]:
    """Get just the Chinese zodiac animal."""
    calc = ZodiacCalculator()
    result = calc.calculate(birth_date)
    return result.get('chinese_animal') if result else None


# Test
if __name__ == '__main__':
    calc = ZodiacCalculator()
    
    # Test with Leonardo DiCaprio's birthday
    result = calc.calculate('1974-11-11')
    print("Leonardo DiCaprio (1974-11-11):")
    print(f"  Western: {result['western_sign']} {result['western_symbol']}")
    print(f"  Element: {result['western_element']}")
    print(f"  Chinese: {result['chinese_animal']} {result['chinese_symbol']}")
    print(f"  Birthstone: {result['birthstone']}")
    print(f"  Traits: {', '.join(result['western_traits'])}")
    print()
    
    # Age calculation
    age = calculate_age('1974-11-11')
    print(f"  Current age: {age}")
    
    # Compatibility test
    compat = calc.get_sign_compatibility('Scorpio', 'Leo')
    print(f"\n  Scorpio + Leo compatibility: {compat['percentage']}% - {compat['description']}")
