import argparse
import pandas as pd
import re
from tqdm import tqdm
# import fitz
import json

VERSION = '0.1'


def fix_pdf_text(old_text):
    """
    fix string from pdf reader
    """
    chr_fix_pair = {'ำ': 'า', '/า': 'ำ', '/้า': '้ำ', '/่': '่'}

    new_text = old_text.strip()
    for o, n in chr_fix_pair.items():
        new_text = new_text.replace(o, n)
    return new_text


def get_patern_of_bullet(in_string):
    regx = [
        ('^0[\-–\d]+$', 70),
        ('^\(\d*(\.?\d*)*\)$', 50),
        ('^[1-9]\d*(\.[1-9]\d*)*\)$', 20),
        ('^[1-9]\d*(\.[1-9]\d*)+$', 5),
        ('^[1-9]\d*\.$', 4),
        ('^โครงการ', 3),
        ('^งาน', 3),
        ('^แผนงาน', 2),
        ('^[\u0E00-\u0E7F]\.$', 1)
    ]
    
    if in_string in ['ด้านการจัดบริการของสำนักงานเขต',
                  'ด้านการบริหารจัดการและบริหารราชการกรุงเทพมหานคร',
                  'ด้านการศึกษา',
                  'ด้านความปลอดภัยและความเป็นระเบียบเรียบร้อย',
                  'ด้านทรัพยากรธรรมชาติและสิ่งแวดล้อม',
                  'ด้านพัฒนาสังคมและชุมชนเมือง',
                  'ด้านการสาธารณสุข',
                  'ด้านสาธารณสุข',
                  'ด้านเมืองและการพัฒนาเมือง',
                  'ด้านการระบายนำและบำบัดนำเสีย',
                  'ด้านการบริหารทั่วไป',
                  'ด้านบริหารทั่วไป',
                  'ด้านเศรษฐกิจและการพาณิชย์']: return ('ด้าน',1)

    for r, l in regx:
        if re.match(r, in_string):
            if l in [5, 20, 50]:
                l = in_string.count('.') + l
            if r == '^งาน' and in_string == 'งานที่จะทำ': continue
            return r, l
    return '', 0

def get_char_type(char):
    char_types = ['[\d\-]', '[\u0E00-\u0E7F]', '[a-zA-Z]']
    
    for i, ctype in enumerate(char_types):
        if re.match(ctype, char) is not None:
            return i
    return -1

def split_text(text):
    text = ['[START]'] + list(text)
    splits = list()
    token = list()
    for ci in range(len(text)-1) :
        c0, c1 = text[ci], text[ci+1]
        if get_char_type(c0) == get_char_type(c1) or c1 in [' ', '.', ','] or c0 in [',', '.'] or c0 == '[START]':
            pass
        else:
            splits.append(''.join(token))
            token = list()
        if c1 != ' ':
            token.append(c1)
    splits.append(''.join(token))
    return splits

def main(args):
    # read csv file
    bb = pd.read_csv(args.csv, index_col=0)
    bb = bb[~bb['pagenum'].isna()]

    bb['pagenum'] = bb['pagenum'].astype(int)
    bb['text'] = bb['text'].astype(str)

    if 'fisical_year' not in bb.columns:
        # add fisical_year and fix_text columns
        bb = bb.assign(
            fisical_year=bb.pdf.apply(lambda v: int(
                re.findall('/6\d/', v)[0][1:-1])),
            fix_text=bb.text.apply(fix_pdf_text)
        )

    if 'fix_text' not in bb.columns:
        # use text from ocr as fix_text
        bb.loc[~bb['ocr-text'].isna(), 'fix_text'] = bb[~bb['ocr-text'].isna()
                                                        ]['ocr-text']

    # add line label if not provided #
    if args.do_add_line_label:
      bb.loc[:, 'line_label'] = -1 
      # preprocess line number
      page_iter = tqdm(bb.groupby('image_path'), desc='parsing line')
      for g, page_df in page_iter:
          page_df = page_df.sort_values(['y0', 'x0'])
          y0_diff = page_df.y0.diff()
          page_iter.desc = f'total word: {len(page_df)}'
          if len(page_df) > 2000: continue
          bb.loc[page_df.index, 'line_label'] = ((y0_diff > 10) | y0_diff.isna()).astype(int).cumsum()

      bb.loc[:, 'line_label'] = bb.line_label.astype(int)
      bb.to_csv('with_line_label.csv', )

    bb_with_entry = bb[bb.line_label != -1]
    
    # mark entry #
    entry_label_id = 0
    is_entry = False
    prog_bar = tqdm(total=len(bb[~bb.line_label.isna()].groupby(['pdf', 'pagenum'])))

    for pdf_name, pdf_df in bb[~bb.line_label.isna()].groupby(['pdf',]): 
        # doc = fitz.open(pdf_name)

        for pdf_page_index, page_df in pdf_df.groupby(['pagenum']):
            prog_bar.desc = f'{pdf_name}:{pdf_page_index}:{len(page_df)}'
            prog_bar.update(1)

            # page = doc.load_page(pdf_page_index)
            # if [x for x in page.get_drawings() if x['rect'].height > 10]:
            #     prog_bar.update(len(pdf_df[pdf_df.pagenum > pdf_page_index].groupby(['pagenum'])))
            #     break

            bb_with_entry.loc[page_df.index, 'is_included'] = True
                
            for line_num, line_df in page_df.groupby('line_label'):
                if (line_df.fix_text == 'รายละเอียดรายจ่าย').any():
                    continue
                
                fisical_year = line_df.fisical_year.values[0]
                if (line_df.fix_text.isin(['ปี', f'25{fisical_year}', 'บาท'])).sum() == 3:
                    continue

                if line_num == 0:
                    continue

                text_line = line_df.sort_values(by='x0',ascending=True).fix_text.values
                text_line = split_text(' '.join(text_line))
                bullet_code = get_patern_of_bullet(text_line[0])[1]

                if bullet_code == 70:
                    if len(text_line) > 1:
                        second_bullet = get_patern_of_bullet(text_line[1])[1]
                        if second_bullet != 0: bullet_code = second_bullet
                    if len(text_line) > 2:
                        third_bullet = get_patern_of_bullet(text_line[2])[1]
                        if third_bullet != 0: bullet_code = third_bullet

                if bullet_code != 0:
                    entry_label_id += 1
                    bb_with_entry.loc[line_df.index, 'bullet_label'] = bullet_code
                    is_entry = True

                if is_entry:
                    bb_with_entry.loc[line_df.index, 'entry_label'] = entry_label_id

                is_last_token_baht = text_line[-1] == 'บาท'
                if is_last_token_baht:
                    is_entry = False
    
    bb_with_entry.to_csv('bkk-budget-with-entry.csv', index=False)
    
    # create indexer #
    entry_bullet_table = bb_with_entry.groupby('entry_label').apply(lambda x: x.bullet_label.unique()[0])
    entry_page_num_table = bb_with_entry.groupby('entry_label').apply(lambda x: x.pagenum.unique()[0])
    entry_text_table = bb_with_entry.groupby(
        'entry_label').apply(lambda x: ' '.join(x.sort_values(['line_label','x0']).fix_text.values))

    budget_resource = json.load(open('budget_resource.json'))
    document_index = {
        pdf['path']: re.sub('^[^\s]* ', '',pdf['name'])
        for year in budget_resource
        for pdf in budget_resource[year]['pdf_path']
    }
    document_url_index = {
        pdf['path']: pdf['url']
        for year in budget_resource
        for pdf in budget_resource[year]['pdf_path']
    }

    child_parent_table = dict()
    curr = None
    prev_pdf_name = None
    for entry, bullet in entry_bullet_table.items():
        pdf_name = bb_with_entry[bb_with_entry.entry_label == entry].pdf.iloc[0]
        if prev_pdf_name != pdf_name:
            curr = None
        prev_pdf_name = pdf_name
        while (curr is not None and bullet <= entry_bullet_table[curr]):
            curr = child_parent_table[curr]
        child_parent_table[entry] = curr
        curr = entry
    
    doc_count = {}
    for doc_name in document_index.values():
        doc_name = re.sub('\s','',doc_name)
        doc_count[doc_name] = doc_count.get(doc_name, 0) + 1
    doc_names = [k for k, v in doc_count.items() if v > 3]

    doc_org_name = {}
    for pdf, pdf_df in bb_with_entry[(bb_with_entry.pagenum == 0)].groupby(['pdf',]):
        for l, line_df in pdf_df.groupby(['line_label']):
            line_text = line_df.fix_text.values.tolist()[0]
            if re.findall('[\u0E00-\u0E7F]+', line_text):
                print(pdf, line_text)
                doc_org_name[pdf] = line_text
                break
    
    rows = []
    for c, p in child_parent_table.items():
        is_leaf = c not in child_parent_table.values()
        if not is_leaf: continue

        curr_p = p
        ancesters = []
        while curr_p is not None:
            ancesters.append(curr_p)
            curr_p = child_parent_table[curr_p]

        ancesters = ancesters[::-1]
        
        pdf_name = bb_with_entry[bb_with_entry.entry_label == c].pdf.iloc[0]
        org = doc_org_name.get(pdf_name)
        ancesters.append(c)
        entries = [entry_text_table.loc[a] for a in ancesters]

        temp_row = dict()
        for i, entry in enumerate(entries):
            
            entry = entry.replace('- 6', '-6')
            amount = re.findall('([\d,]+) ?บาท', entry)
            entry = re.sub('[\d,]+ ?บาท', '', entry)
            bullet = re.findall('^[\d\.\-–]+',entry)
            entry = re.sub('^[\d\.\-–]+', '',entry).strip()
            entry = re.sub('^([\d\.]+|[\u0E00-\u0E7F]\.) ?', '',entry)
    #         entry = re.sub('[\d\u0E00-\u0E7F\.]+', '',entry)
            entry = entry.strip()

            if i == len(entries) - 1:
                if bullet:
                    temp_row['output/proj'] = bullet[0]
                temp_row['output_proj_name'] = entry
                if amount:
                    temp_row['amount'] = amount[0]
            else:
                if bullet:
                    temp_row[f'bullet_{i}'] = bullet[0]
                temp_row[f'name_{i}'] = entry
                if amount:
                    temp_row[f'amount_{i}'] = amount[0]

        row = {'name_organization': org, 'pdf_name': pdf_name}
        row.update(temp_row)
        rows.append(row)

    bkk_budget = pd.DataFrame(rows)
    bkk_budget = bkk_budget.assign(pdf_link=bkk_budget.pdf_name.apply(lambda name: document_url_index[name[4:]]))
    bkk_budget_proj = bkk_budget[bkk_budget['output/proj'].apply(lambda bullet: len(str(bullet)) > 7)]
    bkk_budget_proj.to_csv(f'bkkbudget_61-64_v{VERSION}.csv')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--csv', help='path to CSV file that contains text in Bangkok Budget document', required=True)
    parser.add_argument(
        '--do_add_line_label',
        help='wheter not add line label to data',
        action='store_true',
        )
    args = parser.parse_args()

    main(args)
