#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
춘천시 데이터 분석 프로젝트 - 샘플 데이터 생성 스크립트

대용량 데이터에서 분석에 필요한 샘플을 추출하여 GitHub에 업로드할 수 있는
작은 데이터셋을 생성합니다.
"""

import pandas as pd
import numpy as np
import os
from pathlib import Path

def create_sample_data():
    """대용량 데이터에서 샘플 데이터를 생성합니다."""
    
    print("🎯 샘플 데이터 생성을 시작합니다...")
    
    # 1. 배수등급_춘천.csv 샘플링 (14MB -> 약 1MB)
    try:
        if os.path.exists('배수등급_춘천.csv'):
            print("📊 배수등급_춘천.csv 샘플링 중...")
            df_drainage = pd.read_csv('배수등급_춘천.csv')
            
            # 데이터 크기 확인
            print(f"   원본 크기: {df_drainage.shape}")
            
            # 랜덤 샘플링 (10% 추출)
            sample_size = max(1000, len(df_drainage) // 10)
            df_drainage_sample = df_drainage.sample(n=sample_size, random_state=42)
            
            # 샘플 저장
            df_drainage_sample.to_csv('배수등급_춘천_sample.csv', index=False, encoding='utf-8')
            print(f"   샘플 크기: {df_drainage_sample.shape}")
            print(f"   저장됨: 배수등급_춘천_sample.csv")
        else:
            print("   ⚠️ 배수등급_춘천.csv 파일이 없습니다.")
    except Exception as e:
        print(f"   ❌ 배수등급_춘천.csv 샘플링 실패: {e}")
    
    # 2. joined_gdf.csv 샘플링 (11MB -> 약 1MB)
    try:
        if os.path.exists('joined_gdf.csv'):
            print("📊 joined_gdf.csv 샘플링 중...")
            df_joined = pd.read_csv('joined_gdf.csv')
            
            print(f"   원본 크기: {df_joined.shape}")
            
            # 랜덤 샘플링 (10% 추출)
            sample_size = max(1000, len(df_joined) // 10)
            df_joined_sample = df_joined.sample(n=sample_size, random_state=42)
            
            # 샘플 저장
            df_joined_sample.to_csv('joined_gdf_sample.csv', index=False, encoding='utf-8')
            print(f"   샘플 크기: {df_joined_sample.shape}")
            print(f"   저장됨: joined_gdf_sample.csv")
        else:
            print("   ⚠️ joined_gdf.csv 파일이 없습니다.")
    except Exception as e:
        print(f"   ❌ joined_gdf.csv 샘플링 실패: {e}")
    
    # 3. build_pop_df_0901.csv 샘플링 (12MB -> 약 1MB)
    try:
        if os.path.exists('build_pop_df_0901.csv'):
            print("📊 build_pop_df_0901.csv 샘플링 중...")
            df_pop = pd.read_csv('build_pop_df_0901.csv')
            
            print(f"   원본 크기: {df_pop.shape}")
            
            # 랜덤 샘플링 (10% 추출)
            sample_size = max(1000, len(df_pop) // 10)
            df_pop_sample = df_pop.sample(n=sample_size, random_state=42)
            
            # 샘플 저장
            df_pop_sample.to_csv('build_pop_df_0901_sample.csv', index=False, encoding='utf-8')
            print(f"   샘플 크기: {df_pop_sample.shape}")
            print(f"   저장됨: build_pop_df_0901_sample.csv")
        else:
            print("   ⚠️ build_pop_df_0901.csv 파일이 없습니다.")
    except Exception as e:
        print(f"   ❌ build_pop_df_0901.csv 샘플링 실패: {e}")
    
    # 4. 중복 데이터 정리
    try:
        if os.path.exists('df_total_0910.csv') and os.path.exists('df_total_0910_.csv'):
            print("📊 중복 데이터 정리 중...")
            
            # 더 작은 파일을 메인으로 사용
            if os.path.getsize('df_total_0910.csv') <= os.path.getsize('df_total_0910_.csv'):
                os.rename('df_total_0910.csv', 'df_total_main.csv')
                os.remove('df_total_0910_.csv')
                print("   df_total_0910.csv를 df_total_main.csv로 변경")
            else:
                os.rename('df_total_0910_.csv', 'df_total_main.csv')
                os.remove('df_total_0910.csv')
                print("   df_total_0910_.csv를 df_total_main.csv로 변경")
        else:
            print("   ⚠️ 중복 데이터 파일이 없습니다.")
    except Exception as e:
        print(f"   ❌ 중복 데이터 정리 실패: {e}")
    
    print("\n✅ 샘플 데이터 생성 완료!")
    print("\n📁 생성된 파일들:")
    
    # 생성된 파일 목록 출력
    sample_files = [
        '배수등급_춘천_sample.csv',
        'joined_gdf_sample.csv', 
        'build_pop_df_0901_sample.csv',
        'df_total_main.csv'
    ]
    
    for file in sample_files:
        if os.path.exists(file):
            size = os.path.getsize(file) / (1024 * 1024)  # MB
            print(f"   📄 {file} ({size:.1f} MB)")
    
    print("\n💡 이제 .gitignore를 업데이트하여 원본 대용량 파일들을 제외하고")
    print("   샘플 파일들만 GitHub에 업로드할 수 있습니다.")

if __name__ == "__main__":
    create_sample_data()
