#!/usr/bin/env python3
"""
HVAC Newsletter Generator
Automated system to curate, process, and send HVAC industry newsletters
"""

import feedparser
import requests
import json
import hashlib
import os
import re
from datetime import datetime, timedelta
from urllib.parse import urljoin
from dataclasses import dataclass, asdict
from typing import List, Dict, Set
import time

@dataclass
class Article:
    title: str
    url: str
    summary: str
    source: str
    published: str
    content_hash: str
    category: str = "General"

class HVACNewsletterGenerator:
    def __init__(self):
        self.beehiiv_api_key = os.getenv('BEEHIIV_API_KEY')
        self.beehiiv_publication_id = os.getenv('BEEHIIV_PUBLICATION_ID')
        
        # HVAC RSS feeds
        self.rss_feeds = {
            'ACHR News': 'https://www.achrnews.com/rss.xml',
            'Contracting Business': 'https://www.contractingbusiness.com/rss.xml',
            'Supply House Times': 'https://www.supplyht.com/rss.xml',
            'HVAC.com': 'https://www.hvac.com/feed/',
            'Facility Executive': 'https://facilityexecutive.com/category/hvac/feed/',
        }
        
        # File paths
        self.processed_file = 'data/processed_articles.json'
        self.template_file = 'data/newsletter_template.html'
        
        # Load processed articles
        self.processed_articles = self.load_processed_articles()

    def load_processed_articles(self) -> Set[str]:
        """Load previously processed article hashes"""
        try:
            with open(self.processed_file, 'r') as f:
                data = json.load(f)
                return set(data.get('processed_hashes', []))
        except (FileNotFoundError, json.JSONDecodeError):
            return set()

    def save_processed_articles(self, new_hashes: Set[str]):
        """Save processed article hashes"""
        os.makedirs(os.path.dirname(self.processed_file), exist_ok=True)
        
        # Combine with existing and keep only last 30 days worth
        all_hashes = self.processed_articles.union(new_hashes)
        
        data = {
            'processed_hashes': list(all_hashes),
            'last_updated': datetime.now().isoformat()
        }
        
        with open(self.processed_file, 'w') as f:
            json.dump(data, f, indent=2)

    def generate_content_hash(self, title: str, content: str) -> str:
        """Generate SHA-256 hash of article content for deduplication"""
        combined = f"{title.lower().strip()}{content.lower().strip()}"
        return hashlib.sha256(combined.encode()).hexdigest()

    def fetch_articles_from_feed(self, feed_url: str, source_name: str) -> List[Article]:
        """Fetch and parse articles from RSS feed"""
        articles = []
        
        try:
            feed = feedparser.parse(feed_url)
            
            for entry in feed.entries[:10]:  # Limit to recent articles
                # Clean and extract content
                title = entry.get('title', '').strip()
                url = entry.get('link', '')
                summary = entry.get('summary', entry.get('description', ''))
                published = entry.get('published', '')
                
                if not title or not url:
                    continue
                
                # Generate content hash for deduplication
                content_hash = self.generate_content_hash(title, summary)
                
                # Skip if already processed
                if content_hash in self.processed_articles:
                    continue
                
                # Categorize article
                category = self.categorize_article(title, summary)
                
                # Create article object
                article = Article(
                    title=title,
                    url=url,
                    summary=self.clean_summary(summary),
                    source=source_name,
                    published=published,
                    content_hash=content_hash,
                    category=category
                )
                
                articles.append(article)
                
        except Exception as e:
            print(f"Error fetching from {source_name}: {e}")
        
        return articles

    def categorize_article(self, title: str, summary: str) -> str:
        """Simple categorization based on keywords"""
        content = f"{title} {summary}".lower()
        
        categories = {
            'Technology': ['smart', 'iot', 'digital', 'automation', 'ai', 'tech'],
            'Refrigeration': ['refrigeration', 'cooling', 'chiller', 'freezer'],
            'Heat Pumps': ['heat pump', 'geothermal', 'air source'],
            'Commercial': ['commercial', 'industrial', 'facility'],
            'Residential': ['residential', 'home', 'homeowner'],
            'Efficiency': ['efficiency', 'energy', 'savings', 'green'],
            'Regulations': ['regulation', 'code', 'standard', 'compliance'],
            'Business': ['business', 'market', 'sales', 'revenue', 'growth']
        }
        
        for category, keywords in categories.items():
            if any(keyword in content for keyword in keywords):
                return category
        
        return 'General'

    def clean_summary(self, summary: str) -> str:
        """Clean and truncate article summary"""
        # Remove HTML tags
        clean_text = re.sub(r'<[^>]+>', '', summary)
        
        # Limit to ~150 characters
        if len(clean_text) > 150:
            clean_text = clean_text[:147] + '...'
        
        return clean_text.strip()

    def fetch_all_articles(self) -> List[Article]:
        """Fetch articles from all RSS feeds"""
        all_articles = []
        
        for source_name, feed_url in self.rss_feeds.items():
            print(f"Fetching from {source_name}...")
            articles = self.fetch_articles_from_feed(feed_url, source_name)
            all_articles.extend(articles)
            time.sleep(1)  # Be respectful to RSS feeds
        
        return all_articles

    def deduplicate_articles(self, articles: List[Article]) -> List[Article]:
        """Remove duplicate articles based on content similarity"""
        seen_hashes = set()
        unique_articles = []
        
        for article in articles:
            if article.content_hash not in seen_hashes:
                seen_hashes.add(article.content_hash)
                unique_articles.append(article)
        
        return unique_articles

    def rank_articles(self, articles: List[Article]) -> List[Article]:
        """Simple ranking based on keywords and recency"""
        
        def score_article(article: Article) -> float:
            score = 0.0
            content = f"{article.title} {article.summary}".lower()
            
            # High-value keywords
            high_value_keywords = [
                'hvac', 'air conditioning', 'heating', 'ventilation',
                'energy efficiency', 'smart thermostat', 'heat pump',
                'refrigeration', 'indoor air quality', 'commercial'
            ]
            
            for keyword in high_value_keywords:
                if keyword in content:
                    score += 1.0
            
            # Bonus for certain sources (adjust as needed)
            source_bonus = {
                'ACHR News': 0.2,
                'Contracting Business': 0.1,
            }
            score += source_bonus.get(article.source, 0)
            
            return score
        
        return sorted(articles, key=score_article, reverse=True)

    def generate_newsletter_html(self, articles: List[Article]) -> str:
        """Generate HTML newsletter content"""
        
        # Group articles by category
        categorized = {}
        for article in articles[:20]:  # Limit to top 20
            category = article.category
            if category not in categorized:
                categorized[category] = []
            categorized[category].append(article)
        
        html_content = f"""
        <h1>HVAC Industry Daily Brief - {datetime.now().strftime('%B %d, %Y')}</h1>
        <p>Your curated selection of the latest HVAC industry news and insights.</p>
        """
        
        for category, cat_articles in categorized.items():
            html_content += f"<h2>{category}</h2>\n"
            
            for article in cat_articles:
                html_content += f"""
                <div style="margin-bottom: 20px; padding: 15px; border-left: 3px solid #007acc;">
                    <h3><a href="{article.url}" style="color: #007acc; text-decoration: none;">
                        {article.title}
                    </a></h3>
                    <p style="color: #666; margin: 5px 0;">
                        <strong>Source:</strong> {article.source}
                    </p>
                    <p style="line-height: 1.6;">
                        {article.summary}
                    </p>
                    <p>
                        <a href="{article.url}" style="color: #007acc;">Read full article →</a>
                    </p>
                </div>
                """
        
        html_content += f"""
        <hr style="margin: 30px 0;">
        <p style="color: #666; font-size: 12px;">
            This newsletter was automatically curated from industry sources. 
            Generated on {datetime.now().strftime('%Y-%m-%d at %H:%M UTC')}.
        </p>
        """
        
        return html_content

    def send_to_beehiiv(self, subject: str, content: str) -> bool:
        """Send newsletter to Beehiiv"""
        if not self.beehiiv_api_key or not self.beehiiv_publication_id:
            print("Beehiiv API credentials not configured")
            return False
        
        url = f"https://api.beehiiv.com/v2/publications/{self.beehiiv_publication_id}/posts"
        
        headers = {
            'Authorization': f'Bearer {self.beehiiv_api_key}',
            'Content-Type': 'application/json'
        }
        
        payload = {
            'title': subject,
            'content_html': content,
            'status': 'draft',  # Create as draft first
            'audience': 'free'
        }
        
        try:
            response = requests.post(url, headers=headers, json=payload)
            print(f"Beehiiv API Response Status: {response.status_code}")
            print(f"Beehiiv API Response: {response.text}")
            response.raise_for_status()
            print("Newsletter sent successfully to Beehiiv!")
            return True
        except requests.exceptions.RequestException as e:
            print(f"Error sending to Beehiiv: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response content: {e.response.text}")
            return False

    def run(self):
        """Main execution pipeline"""
        print("Starting HVAC Newsletter Generation...")
        
        # 1. Fetch articles
        print("Fetching articles from RSS feeds...")
        articles = self.fetch_all_articles()
        print(f"Fetched {len(articles)} new articles")
        
        if not articles:
            print("No new articles found. Exiting.")
            return
        
        # 2. Deduplicate
        print("Removing duplicates...")
        unique_articles = self.deduplicate_articles(articles)
        print(f"After deduplication: {len(unique_articles)} unique articles")
        
        # 3. Rank and select top articles
        print("Ranking articles...")
        ranked_articles = self.rank_articles(unique_articles)
        
        # 4. Generate newsletter
        print("Generating newsletter content...")
        newsletter_html = self.generate_newsletter_html(ranked_articles)
        
        # 5. Send to Beehiiv
        subject = f"HVAC Daily Brief - {datetime.now().strftime('%B %d, %Y')}"
        success = self.send_to_beehiiv(subject, newsletter_html)
        
        if success:
            # 6. Save processed article hashes
            new_hashes = {article.content_hash for article in unique_articles}
            self.save_processed_articles(new_hashes)
            print("Newsletter generation completed successfully!")
        else:
            print("Newsletter generation failed.")

if __name__ == "__main__":
    generator = HVACNewsletterGenerator()
    generator.run()
